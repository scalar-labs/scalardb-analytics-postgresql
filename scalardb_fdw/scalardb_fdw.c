#include "c.h"
#include "option.h"
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_type_d.h"
#include "commands/explain.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "utils/rel.h"

#include "scalardb.h"
#include "scalardb_fdw.h"

PG_MODULE_MAGIC;

/*
 * The plan state is set up in scalardbGetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in scalardbGetForeignPaths.
 */
typedef struct {
    ScalarDBFdwOptions options;

    /* Bitmap of attr numbers we need to fetch from the remote server. */
    Bitmapset* attrs_used;

    /*
     * Restriction clauses, divided into safe and unsafe to pushdown subsets.
     * All entries in these lists should have RestrictInfo wrappers; that
     * improves efficiency of selectivity and cost estimation.
     */
    List* remote_conds;
    List* local_conds;

    BlockNumber pages; /* estimate of physical size */
    double tuples;     /* estimate of number of data rows */
} ScalarDBFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct {
    ScalarDBFdwOptions options;

    /* extracted fdw_private data */
    List* attrs_to_retrieve; /* list of retrieved attribute numbers */

    Relation rel;             /* relcache entry for the foreign table */
    AttInMetadata* attinmeta; /* attribute datatype conversion metadata */

    jobject scanner; /* Java instance of com.scalar.db.api.Scanner */
} ScalarDBFdwScanState;

enum ScanFdwPrivateIndex {
    /* Integer list of attribute numbers retrieved by the SELECT */
    ScanFdwPrivateAttrsToRetrieve
};

static void classifyConditions(PlannerInfo* root, RelOptInfo* baserel,
                               List* input_conds, List** remote_conds,
                               List** local_conds);

static bool is_foreign_expr(PlannerInfo* root, RelOptInfo* baserel, Expr* expr);

static void estimate_size(PlannerInfo* root, RelOptInfo* baserel,
                          ScalarDBFdwPlanState* fdw_private);
static void estimate_costs(PlannerInfo* root, RelOptInfo* baserel,
                           Cost* startup_cost, Cost* total_cost);

static void get_target_list(PlannerInfo* root, RelOptInfo* baserel,
                            Bitmapset* attrs_used, List** attrs_to_retrieve);

static Datum convert_result_column_to_datum(jobject result, char* attname,
                                            Oid atttypid);

static HeapTuple make_tuple_from_result(jobject result, int ncolumn,
                                        Relation rel, List* attrs_to_retrieve);

PG_FUNCTION_INFO_V1(scalardb_fdw_handler);

Datum scalardb_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine* routine = makeNode(FdwRoutine);

    /* Functions for scanning foreign tables */
    routine->GetForeignRelSize = scalardbGetForeignRelSize;
    routine->GetForeignPaths = scalardbGetForeignPaths;
    routine->GetForeignPlan = scalardbGetForeignPlan;
    routine->BeginForeignScan = scalardbBeginForeignScan;
    routine->IterateForeignScan = scalardbIterateForeignScan;
    routine->ReScanForeignScan = scalardbReScanForeignScan;
    routine->EndForeignScan = scalardbEndForeignScan;

    /* Support functions for EXPLAIN */
    routine->ExplainForeignScan = scalardbExplainForeignScan;

    /* Support functions for ANALYZE */
    routine->AnalyzeForeignTable = scalardbAnalyzeForeignTable;
    PG_RETURN_POINTER(routine);
}

static void scalardbGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel,
                                      Oid foreigntableid) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    baserel->rows = 0;

    ScalarDBFdwPlanState* fdw_private = palloc0(sizeof(ScalarDBFdwPlanState));
    baserel->fdw_private = (void*)fdw_private;

    get_scalardb_fdw_options(foreigntableid, &fdw_private->options);

    classifyConditions(root, baserel, baserel->baserestrictinfo,
                       &fdw_private->remote_conds, &fdw_private->local_conds);

    /*
     * Identify which attributes will need to be retrieved from the remote
     * server.  These include all attrs needed for joins or final output,
     * plus all attrs used in the local_conds.  (Note: if we end up using a
     * parameterized scan, it's possible that some of the join clauses will
     * be sent to the remote and thus we wouldn't really need to retrieve
     * the columns used in them.  Doesn't seem worth detecting that case
     * though.)
     */
    fdw_private->attrs_used = NULL;
    pull_varattnos((Node*)baserel->reltarget->exprs, baserel->relid,
                   &fdw_private->attrs_used);

    ListCell* lc;
    foreach (lc, fdw_private->local_conds) {
        RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

        pull_varattnos((Node*)rinfo->clause, baserel->relid,
                       &fdw_private->attrs_used);
    }

    /* Estimate relation size */
    estimate_size(root, baserel, fdw_private);
}

/*
 * scalardbGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 */
static void scalardbGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel,
                                    Oid foreigntableid) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    // ScalarDBFdwPlanState* fdw_private =
    //     (ScalarDBFdwPlanState*)baserel->fdw_private;
    Cost startup_cost;
    Cost total_cost;

    /* Estimate costs */
    estimate_costs(root, baserel, &startup_cost, &total_cost);

    /* Create a ForeignPath node corresponding to Scan.all()
     * and add it as only possible path */
    ForeignPath* path =
        create_foreignscan_path(root, baserel, NULL, /* default pathtarget */
                                baserel->rows,       /* number of rows */
                                startup_cost,        /* startup cost */
                                total_cost,          /* total cost */
                                NIL,                 /* no pathkeys */
                                NULL,                /* no outer rel either */
                                NULL,                /* no extra plan */
                                NIL);                /* no fdw_private */
    add_path(baserel, (Path*)path);
    /* TODO: support other type of Scan, including partitionKey() and
     * indexKey()
     */
    /* TODO: support ordering push down */
}

static ForeignScan*
scalardbGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel,
                       Oid foreigntableid, ForeignPath* best_path, List* tlist,
                       List* scan_clauses, Plan* outer_plan) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    ScalarDBFdwPlanState* fdw_private =
        (ScalarDBFdwPlanState*)baserel->fdw_private;
    Index scan_relid;
    List* remote_exprs = NIL;
    List* local_exprs = NIL;
    List* fdw_recheck_quals = NIL;

    List* fdw_private_for_scan = NIL;

    /* So far, baserel is always base relations because
     * GetForeignJoinPaths nor GetForeignUpperPaths are not defined.
     */

    /*
     * For base relations, set scan_relid as the relid of the relation.
     */
    scan_relid = baserel->relid;

    /*
     * In a base-relation scan, we must apply the given scan_clauses.
     *
     * Separate the scan_clauses into those that can be executed remotely
     * and those that can't.  baserestrictinfo clauses that were
     * previously determined to be safe or unsafe by classifyConditions
     * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
     * else in the scan_clauses list will be a join clause, which we have
     * to check for remote-safety.
     *
     * Note: the join clauses we see here should be the exact same ones
     * previously examined by postgresGetForeignPaths.  Possibly it'd be
     * worth passing forward the classification work done then, rather
     * than repeating it here.
     *
     * This code must match "extract_actual_clauses(scan_clauses, false)"
     * except for the additional decision about remote versus local
     * execution.
     */
    ListCell* lc;
    foreach (lc, scan_clauses) {
        RestrictInfo* rinfo = lfirst_node(RestrictInfo, lc);

        /* Ignore any pseudoconstants, they're dealt with elsewhere */
        if (rinfo->pseudoconstant)
            continue;

        if (list_member_ptr(fdw_private->remote_conds, rinfo))
            remote_exprs = lappend(remote_exprs, rinfo->clause);
        else if (list_member_ptr(fdw_private->local_conds, rinfo))
            local_exprs = lappend(local_exprs, rinfo->clause);
        else if (is_foreign_expr(root, baserel, rinfo->clause))
            remote_exprs = lappend(remote_exprs, rinfo->clause);
        else
            local_exprs = lappend(local_exprs, rinfo->clause);
    }

    /*
     * For a base-relation scan, we have to support EPQ recheck, which
     * should recheck all the remote quals.
     */
    fdw_recheck_quals = remote_exprs;

    List* attrs_to_retrieve = NIL;
    get_target_list(root, baserel, fdw_private->attrs_used, &attrs_to_retrieve);

    fdw_private_for_scan = list_make1(attrs_to_retrieve);

    /*
     * Create the ForeignScan node for the given relation.
     *
     * Note that the remote parameter expressions are stored in the
     * fdw_exprs field of the finished plan node; we can't keep them in
     * private state because then they wouldn't be subject to later planner
     * processing.
     */
    return make_foreignscan(tlist, local_exprs, scan_relid,
                            NIL, /* no expressions to evaluate */
                            fdw_private_for_scan, /* private state */
                            NIL,                  /* no custom tlist */
                            fdw_recheck_quals,    /* remote quals */
                            outer_plan);
}

static void scalardbBeginForeignScan(ForeignScanState* node, int eflags) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    EState* estate = node->ss.ps.state;
    RangeTblEntry* rte;

    ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;
    ScalarDBFdwScanState* fdw_state;

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);

    fdw_state = (ScalarDBFdwScanState*)palloc0(sizeof(ScalarDBFdwScanState));
    node->fdw_state = (void*)fdw_state;

    get_scalardb_fdw_options(rte->relid, &fdw_state->options);

    /* Get private info created by planner functions. */
    fdw_state->attrs_to_retrieve =
        (List*)list_nth(fsplan->fdw_private, ScanFdwPrivateAttrsToRetrieve);

    /* Get info we'll need for input data conversion. */
    fdw_state->rel = node->ss.ss_currentRelation;
    fdw_state->attinmeta =
        TupleDescGetAttInMetadata(RelationGetDescr(fdw_state->rel));

    scalardb_initialize(&fdw_state->options);

    fdw_state->scanner = NULL;
}

static TupleTableSlot* scalardbIterateForeignScan(ForeignScanState* node) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    ScalarDBFdwScanState* fdw_state = (ScalarDBFdwScanState*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    if (!fdw_state->scanner) {
        fdw_state->scanner = scalardb_scan_all(fdw_state->options.namespace,
                                               fdw_state->options.table_name);
    }

    jobject result_optional = scalardb_scanner_one(fdw_state->scanner);

    if (!scalardb_optional_is_present(result_optional)) {
        return ExecClearTuple(slot);
    }

    jobject result = scalardb_optional_get(result_optional);
    HeapTuple tuple =
        make_tuple_from_result(result, scalardb_result_columns_size(result),
                               fdw_state->rel, fdw_state->attrs_to_retrieve);

    ExecStoreHeapTuple(tuple, slot, false);

    return slot;
}

static void scalardbReScanForeignScan(ForeignScanState* node) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    ScalarDBFdwScanState* fdw_state = (ScalarDBFdwScanState*)node->fdw_state;
    if (!fdw_state->scanner)
        scalardb_scanner_close(fdw_state->scanner);

    fdw_state->scanner = scalardb_scan_all(fdw_state->options.namespace,
                                           fdw_state->options.table_name);
}

static void scalardbEndForeignScan(ForeignScanState* node) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    ScalarDBFdwScanState* fdw_state = (ScalarDBFdwScanState*)node->fdw_state;

    /* if fdw_state is NULL, we are in EXPLAIN; nothing to do */
    if (fdw_state == NULL)
        return;

    /* Close the scanner if open, to prevent accumulation of cursors */
    if (fdw_state->scanner)
        scalardb_scanner_close(fdw_state->scanner);

    // TODO: consider whether DistributedStorage should be closed
    // here
}

static void scalardbExplainForeignScan(ForeignScanState* node,
                                       ExplainState* es) {}

static bool scalardbAnalyzeForeignTable(Relation relation,
                                        AcquireSampleRowsFunc* func,
                                        BlockNumber* totalpages) {
    return false;
}

/*
 * Examine each qual clause in input_conds, and classify them into two
 * groups, which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
static void classifyConditions(PlannerInfo* root, RelOptInfo* baserel,
                               List* input_conds, List** remote_conds,
                               List** local_conds) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    ListCell* lc;

    *remote_conds = NIL;
    *local_conds = NIL;

    foreach (lc, input_conds) {
        RestrictInfo* ri = lfirst_node(RestrictInfo, lc);

        if (is_foreign_expr(root, baserel, ri->clause))
            *remote_conds = lappend(*remote_conds, ri);
        else
            *local_conds = lappend(*local_conds, ri);
    }
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
static bool is_foreign_expr(PlannerInfo* root, RelOptInfo* baserel,
                            Expr* expr) {
    /* TODO: implement this */
    return false;
}

/*
 * Estimate the size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void estimate_size(PlannerInfo* root, RelOptInfo* baserel,
                          ScalarDBFdwPlanState* fdw_private) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    /*
     * If the foreign table has never been ANALYZEd, it will have
     * reltuples < 0, meaning "unknown". In this case we can use a hack
     * similar to plancat.c's treatment of empty relations: use a minimum
     * size estimate of 10 pages, and divide by the column-datatype-based
     * width estimate to get the corresponding number of tuples.
     */
#if PG_VERSION_NUM >= 140000
    if (baserel->tuples < 0) {
#else
    if (baserel->pages == 0 && baserel->tuples == 0) {
#endif
        baserel->pages = 10;
        baserel->tuples = (10.0 * BLCKSZ) / (baserel->reltarget->width +
                                             sizeof(HeapTupleHeaderData));
    } else {
        /* TODO: support ANALYZEd table */
        ereport(ERROR,
                errmsg("foreign table of scalardb_fdw should not be ANALYZEd"));
    }

    /* Estimate baserel size as best we can with local statistics. */
    set_baserel_size_estimates(root, baserel);
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void estimate_costs(PlannerInfo* root, RelOptInfo* baserel,
                           Cost* startup_cost, Cost* total_cost) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    /* TODO: consider whether more precise estimation of cost is worth doing
     */
    int cost_per_tuple = 1;
    *startup_cost = 100; /* temporary default value */
    *total_cost = *startup_cost + cost_per_tuple * baserel->tuples;
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 *
 * This retunrns an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
static void get_target_list(PlannerInfo* root, RelOptInfo* baserel,
                            Bitmapset* attrs_used, List** attrs_to_retrieve) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));

    RangeTblEntry* rte = planner_rt_fetch(baserel->relid, root);

    /*
     * Core code already has some lock on each rel being planned, so we can
     * use NoLock here.
     */
    Relation rel = table_open(rte->relid, NoLock);
    TupleDesc tupdesc = RelationGetDescr(rel);

    *attrs_to_retrieve = NIL;

    /* If there's a whole-row reference, we'll need all the columns. */
    bool have_wholerow =
        bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

    int i;
    for (i = 1; i <= tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

        /* Ignore dropped attributes. */
        if (attr->attisdropped)
            continue;

        if (have_wholerow ||
            bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
            *attrs_to_retrieve = lappend_int(*attrs_to_retrieve, i);
        }
    }
    table_close(rel, NoLock);
}

static HeapTuple make_tuple_from_result(jobject result, int ncolumn,
                                        Relation rel, List* attrs_to_retrieve) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    TupleDesc tupdesc = RelationGetDescr(rel);

    Datum* values = (Datum*)palloc0(tupdesc->natts * sizeof(Datum));
    bool* nulls = (bool*)palloc(tupdesc->natts * sizeof(bool));

    /* Initialize to nulls for any columns not present in result */
    memset(nulls, false, tupdesc->natts * sizeof(bool));

    /*
     * i indexes columns in the relation, j indexes columns in the PGresult.
     */
    int count = 0;
    ListCell* lc;
    foreach (lc, attrs_to_retrieve) {
        int i = lfirst_int(lc);

        if (i > 0) {
            /* ordinary column */
            Assert(i <= tupdesc->natts);
            FormData_pg_attribute attr = tupdesc->attrs[i - 1];
            char* attname = NameStr(attr.attname);
            ereport(DEBUG1, errmsg("i %d, attname %s", i, attname));
            nulls[i - 1] = (scalardb_result_is_null(result, attname));
            values[i - 1] =
                convert_result_column_to_datum(result, attname, attr.atttypid);
        }

        count++;
    }

    /* TODO: retrieve only used columns */
    /*
     * Check we got the expected number of columns.  Note: j == 0 and
     * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
     */
    // if (count > 0 && count != ncolumn)
    //     elog(ERROR, "remote query result does not match the foreign table");

    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);

    return tuple;
}

static Datum convert_result_column_to_datum(jobject result, char* attname,
                                            Oid atttypid) {
    ereport(DEBUG1, errmsg("entering function %s", __func__));
    switch (atttypid) {
    case BOOLOID: {
        bool val = scalardb_result_get_boolean(result, attname);
        return BoolGetDatum(val);
    }
    case INT4OID: {
        int val = scalardb_result_get_int(result, attname);
        return Int32GetDatum(val);
    }
    case INT8OID: {
        int val = scalardb_result_get_bigint(result, attname);
        return Int64GetDatum(val);
    }
    case FLOAT4OID: {
        int val = scalardb_result_get_float(result, attname);
        return Float4GetDatum(val);
    }
    case FLOAT8OID: {
        int val = scalardb_result_get_double(result, attname);
        return Float8GetDatum(val);
    }
    case TEXTOID: {
        text* val = scalardb_result_get_text(result, attname);
        PG_RETURN_TEXT_P(val);
    }
    case BYTEAOID: {
        bytea* val = scalardb_result_get_blob(result, attname);
        PG_RETURN_BYTEA_P(val);
    }
    default:
        ereport(ERROR, errmsg("Unsupported data type: %d", atttypid));
    }
}
