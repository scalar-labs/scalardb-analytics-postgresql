#include "c.h"
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_type_d.h"
#include "commands/explain.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "nodes/pathnodes.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "parser/parse_node.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

#include "scalardb.h"
#include "scalardb_fdw.h"
#include "scalardb_fdw_util.h"
#include "condition.h"
#include "option.h"

PG_MODULE_MAGIC;

/*
 * The plan state is set up in scalardbGetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in scalardbGetForeignPaths.
 */
typedef struct {
	ScalarDbFdwOptions options;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	List *remote_conds;
	List *local_conds;

	/* estimate of physical size */
	BlockNumber pages;
	/* estimate of number of data rows */
	double tuples;

	/* set of the column metadata of the table*/
	ScalarDbFdwColumnMetadata column_metadata;
} ScalarDbFdwPlanState;

/* 
 * Scan Operation Type
 */
typedef enum {
	SCALARDB_FDW_SCAN_ALL,
	SCALARDB_FDW_SCAN_PARTITION_KEY,
	SCALARDB_FDW_SCAN_SECONDARY_INDEX,
} ScalarDbFdwScanType;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct {
	ScalarDbFdwOptions options;

	/* extracted fdw_private data. See the following enum for the content*/
	List *attrs_to_retrieve;
	List *condition_key_names;
	List *condition_key_types;
	List *condition_operators;

	/* List of retrieved attribute names, coverted from attrs_to_retrieve */
	List *attnames;
	/* relcache entry for the foreign table */
	Relation rel;
	/* attribute datatype conversion metadata */
	AttInMetadata *attinmeta;

	/* Array of Conditions for ScalarDB Scan */
	ScalarDbFdwScanCondition *scan_conds;
	/* number of conditions in scan_conds */
	size_t scan_conds_len;
	/* Scan operation type */
	ScalarDbFdwScanType scan_type;

	/* Java instance of com.scalar.db.api.Scan.*/
	jobject scan;
	/* Java instance of com.scalar.db.api.Scanner */
	jobject scanner;
} ScalarDbFdwScanState;

enum ScanFdwPrivateIndex {
	/* Integer list of attribute numbers retrieved by the SELECT */
	ScanFdwPrivateAttrsToRetrieve,
	/* List of String that contains column names of the lhs of the pushed-down conditions */
	ScanFdwPrivateConditionColumnNames,
	/* List of ScalarDbFdwConditionKeyType, which contains types of the keys of the pushed-down conditions */
	ScanFdwPrivateConditionKeyTypes,
	/* List of ScalarDbFdwConditionOperator, which contains operators of the pushed-down conditions */
	ScanFdwPrivateConditionOperators,
};

static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  ScalarDbFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   List *remote_conds, Cost *startup_cost,
			   Cost *total_cost);

static void get_target_list(PlannerInfo *root, RelOptInfo *baserel,
			    Bitmapset *attrs_used, List **attrs_to_retrieve);

static void get_attnames(TupleDesc tupdesc, List *attrs_to_retrieve,
			 List **attnames);

static Datum convert_result_column_to_datum(jobject result, char *attname,
					    Oid atttypid);

static HeapTuple make_tuple_from_result(jobject result, Relation rel,
					List *attrs_to_retrieve);

static ScalarDbFdwScanCondition *
prepare_scan_conds(ForeignScanState *node, List *fdw_expr, List *key_names,
		   List *key_types, List *operators);

static List *exprs_to_strings(ScalarDbFdwScanCondition *scan_conds,
			      size_t num_conds);

static ScalarDbFdwScanType determine_scan_type(List *condition_types);

PG_FUNCTION_INFO_V1(scalardb_fdw_handler);

Datum scalardb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

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

static void scalardbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
				      Oid foreigntableid)
{
	ScalarDbFdwPlanState *fdw_private;
	ListCell *lc;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	baserel->rows = 0;

	fdw_private = palloc0(sizeof(ScalarDbFdwPlanState));
	baserel->fdw_private = (void *)fdw_private;

	get_scalardb_fdw_options(foreigntableid, &fdw_private->options);

	scalardb_initialize(&fdw_private->options);

	get_column_metadata(root, baserel, fdw_private->options.namespace,
			    fdw_private->options.table_name,
			    &fdw_private->column_metadata);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server. These include all attrs needed for attrs used in the local_conds.
	 */
	fdw_private->attrs_used = NULL;
	pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
		       &fdw_private->attrs_used);

	foreach(lc, fdw_private->local_conds) {
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *)rinfo->clause, baserel->relid,
			       &fdw_private->attrs_used);
	}

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
}

/*
 * scalardbGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 */
static void scalardbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
				    Oid foreigntableid)
{
	ForeignPath *path;
	Cost startup_cost;
	Cost total_cost;

	ScalarDbFdwPlanState *fdw_private =
		(ScalarDbFdwPlanState *)baserel->fdw_private;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	/* 
	 * Separate baserestrictinfo into two groups:
	 * 1. remote_conds: conditions that will be pushed down to ScalarDB
	 * 2. local_conds: conditions that will be evaluated locally
	 */
	determine_remote_conds(baserel, baserel->baserestrictinfo,
			       &fdw_private->column_metadata,
			       &fdw_private->remote_conds,
			       &fdw_private->local_conds);

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private->remote_conds, &startup_cost,
		       &total_cost);

	/* Create a ForeignPath node corresponding to Scan
	 * and add it as only possible path */
	path = create_foreignscan_path(root, baserel,
				       NULL, /* default pathtarget */
				       baserel->rows, /* number of rows */
				       startup_cost, /* startup cost */
				       total_cost, /* total cost */
				       NIL, /* no pathkeys */
				       NULL, /* no outer rel either */
				       NULL, /* no extra plan */
				       NIL); /* no fdw_private */
	add_path(baserel, (Path *)path);
}

static ForeignScan *scalardbGetForeignPlan(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid,
					   ForeignPath *best_path, List *tlist,
					   List *scan_clauses, Plan *outer_plan)
{
	ScalarDbFdwPlanState *fdw_private;
	ListCell *lc;
	List *attrs_to_retrieve = NIL;

	Index scan_relid;
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *fdw_recheck_quals = NIL;

	List *fdw_private_for_scan = NIL;

	List *fdw_exprs = NIL;
	bool shippable;
	ScalarDbFdwShippableCondition shippable_cond;
	List *condition_key_names = NIL;
	List *condition_key_types = NIL;
	List *condition_operators = NIL;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	fdw_private = (ScalarDbFdwPlanState *)baserel->fdw_private;

	/* So far, baserel is always base relations because
	 * GetForeignJoinPaths nor GetForeignUpperPaths are not defined.
	 */

	/* For base relations, set scan_relid as the relid of the relation. */
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
	foreach(lc, scan_clauses) {
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fdw_private->remote_conds, rinfo)) {
			remote_exprs = lappend(remote_exprs, rinfo->clause);

			shippable = is_shippable_condition(
				baserel, &fdw_private->column_metadata,
				rinfo->clause, &shippable_cond);

			/* This must be true since remote_conds contains only shippable conditions*/
			if (shippable) {
				fdw_exprs =
					lappend(fdw_exprs, shippable_cond.expr);
				condition_key_names =
					lappend(condition_key_names,
						shippable_cond.name);
				condition_key_types =
					lappend_int(condition_key_types,
						    shippable_cond.key);
				condition_operators = lappend_int(
					condition_operators, shippable_cond.op);
			}
		} else if (list_member_ptr(fdw_private->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else
			/* join clauses */
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	/*
	 * For a base-relation scan, we have to support EPQ recheck, which
	 * should recheck all the remote quals.
	 */
	fdw_recheck_quals = remote_exprs;

	get_target_list(root, baserel, fdw_private->attrs_used,
			&attrs_to_retrieve);

	fdw_private_for_scan =
		list_make4(attrs_to_retrieve, condition_key_names,
			   condition_key_types, condition_operators);

	return make_foreignscan(tlist, local_exprs, scan_relid, fdw_exprs,
				fdw_private_for_scan, /* private state */
				NIL, /* no custom tlist */
				fdw_recheck_quals, /* remote quals */
				outer_plan);
}

static void scalardbBeginForeignScan(ForeignScanState *node, int eflags)
{
	EState *estate;
	ForeignScan *fsplan;
	RangeTblEntry *rte;
	ScalarDbFdwScanState *fdw_state;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	estate = node->ss.ps.state;

	fsplan = (ForeignScan *)node->ss.ps.plan;

	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);

	fdw_state =
		(ScalarDbFdwScanState *)palloc0(sizeof(ScalarDbFdwScanState));
	node->fdw_state = (void *)fdw_state;

	get_scalardb_fdw_options(rte->relid, &fdw_state->options);

	/* Get private info created by planner functions. */
	fdw_state->attrs_to_retrieve = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateAttrsToRetrieve);

	fdw_state->condition_key_names = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateConditionColumnNames);

	fdw_state->condition_key_types = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateConditionKeyTypes);

	fdw_state->condition_operators = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateConditionOperators);

	/* Get info we'll need for input data conversion. */
	fdw_state->rel = node->ss.ss_currentRelation;
	fdw_state->attinmeta =
		TupleDescGetAttInMetadata(RelationGetDescr(fdw_state->rel));

	get_attnames(fdw_state->attinmeta->tupdesc,
		     fdw_state->attrs_to_retrieve, &fdw_state->attnames);

	/* Prepare conditions for Scan */
	fdw_state->scan_conds = prepare_scan_conds(
		node, fsplan->fdw_exprs, fdw_state->condition_key_names,
		fdw_state->condition_key_types, fdw_state->condition_operators);
	fdw_state->scan_conds_len = list_length(fsplan->fdw_exprs);

	fdw_state->scan_type =
		determine_scan_type(fdw_state->condition_key_types);

	/* Instanciate Scan object of ScalarDb*/
	switch (fdw_state->scan_type) {
	case SCALARDB_FDW_SCAN_ALL:
		fdw_state->scan = scalardb_scan_all(
			fdw_state->options.namespace,
			fdw_state->options.table_name, fdw_state->attnames);
		break;
	case SCALARDB_FDW_SCAN_PARTITION_KEY:
		fdw_state->scan = scalardb_scan(fdw_state->options.namespace,
						fdw_state->options.table_name,
						fdw_state->attnames,
						fdw_state->scan_conds,
						fdw_state->scan_conds_len);
		break;
	case SCALARDB_FDW_SCAN_SECONDARY_INDEX:
		fdw_state->scan = scalardb_scan_with_index(
			fdw_state->options.namespace,
			fdw_state->options.table_name, fdw_state->attnames,
			fdw_state->scan_conds, fdw_state->scan_conds_len);
		break;
	}

	fdw_state->scanner = NULL;
}

static TupleTableSlot *scalardbIterateForeignScan(ForeignScanState *node)
{
	ScalarDbFdwScanState *fdw_state;
	TupleTableSlot *slot;
	jobject result_optional;
	jobject result;
	HeapTuple tuple;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	fdw_state = (ScalarDbFdwScanState *)node->fdw_state;
	slot = node->ss.ss_ScanTupleSlot;

	if (!fdw_state->scanner) {
		fdw_state->scanner = scalardb_start_scan(fdw_state->scan);
	}

	result_optional = scalardb_scanner_one(fdw_state->scanner);

	if (!scalardb_optional_is_present(result_optional)) {
		return ExecClearTuple(slot);
	}

	result = scalardb_optional_get(result_optional);
	tuple = make_tuple_from_result(result, fdw_state->rel,
				       fdw_state->attrs_to_retrieve);

	scalardb_scanner_release_result();

	ExecStoreHeapTuple(tuple, slot, false);

	return slot;
}

static void scalardbReScanForeignScan(ForeignScanState *node)
{
	ScalarDbFdwScanState *fdw_state;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	fdw_state = (ScalarDbFdwScanState *)node->fdw_state;
	if (fdw_state->scanner)
		scalardb_scanner_close(fdw_state->scanner);

	fdw_state->scanner = scalardb_start_scan(fdw_state->scan);
}

static void scalardbEndForeignScan(ForeignScanState *node)
{
	ScalarDbFdwScanState *fdw_state;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	fdw_state = (ScalarDbFdwScanState *)node->fdw_state;

	if (fdw_state->scan)
		scalardb_release_scan(fdw_state->scan);

	/* Close the scanner if open, to prevent accumulation of scanner */
	if (fdw_state->scanner)
		scalardb_scanner_close(fdw_state->scanner);

	// TODO: consider whether DistributedStorage should be closed
	// here
}

static void scalardbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ScalarDbFdwScanState *fdw_state;
	ListCell *lc;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	fdw_state = (ScalarDbFdwScanState *)node->fdw_state;
	ExplainPropertyText("ScalarDB Namespace", fdw_state->options.namespace,
			    es);
	ExplainPropertyText("ScalarDB Table", fdw_state->options.table_name,
			    es);
	if (es->verbose) {
		char *scan_type_str;
		switch (fdw_state->scan_type) {
		case SCALARDB_FDW_SCAN_ALL:
			scan_type_str = "all";
			break;
		case SCALARDB_FDW_SCAN_PARTITION_KEY:
			scan_type_str = "partition key";
			break;
		case SCALARDB_FDW_SCAN_SECONDARY_INDEX:
			scan_type_str = "secondary index";
			break;
		}

		ExplainPropertyText("ScalarDB Scan Type", scan_type_str, es);

		if (list_length(fdw_state->attnames) > 0)
			ExplainPropertyText("ScalarDB Scan Attribute",
					    nodeToString(fdw_state->attnames),
					    es);

		if (fdw_state->scan_conds_len > 0) {
			List *expr_strings =
				exprs_to_strings(fdw_state->scan_conds,
						 fdw_state->scan_conds_len);

			foreach(lc, expr_strings) {
				char *expr_str = strVal(lfirst(lc));
				ExplainPropertyText("ScalarDB Scan Conditions",
						    expr_str, es);
			}
		}
	}
}

static bool scalardbAnalyzeForeignTable(Relation relation,
					AcquireSampleRowsFunc *func,
					BlockNumber *totalpages)
{
	return false;
}

/*
 * Estimate the size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  ScalarDbFdwPlanState *fdw_private)
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));
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
		baserel->tuples =
			(10.0 * BLCKSZ) / (baserel->reltarget->width +
					   sizeof(HeapTupleHeaderData));
	} else {
		ereport(ERROR,
			errmsg("foreign table of scalardb_fdw should not be ANALYZEd"));
	}

	/* Estimate baserel size as best we can with local statistics. */
	set_baserel_size_estimates(root, baserel);
}

#define DEFAULT_ROWS_FOR_PARTITION_KEY_SCAN 10

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   List *remote_conds, Cost *startup_cost,
			   Cost *total_cost)
{
	Cost run_cost = 0;
	Cost cpu_per_tuple;
	double rows;
	ScalarDbFdwPlanState *fdw_private =
		(ScalarDbFdwPlanState *)baserel->fdw_private;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	rows = list_length(fdw_private->remote_conds) > 0 ?
		       DEFAULT_ROWS_FOR_PARTITION_KEY_SCAN :
		       baserel->rows;

	*startup_cost = 0;
	*startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	/* Add in tlist eval cost for each output row */
	*startup_cost += baserel->reltarget->cost.startup;
	run_cost += baserel->reltarget->cost.per_tuple * rows;

	*total_cost = *startup_cost + run_cost;
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 *
 * This retunrns an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
static void get_target_list(PlannerInfo *root, RelOptInfo *baserel,
			    Bitmapset *attrs_used, List **attrs_to_retrieve)
{
	RangeTblEntry *rte;
	Relation rel;
	TupleDesc tupdesc;
	bool have_wholerow;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	rte = planner_rt_fetch(baserel->relid, root);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);

	*attrs_to_retrieve = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
				      attrs_used);

	for (int i = 1; i <= tupdesc->natts; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
		    bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
				  attrs_used)) {
			*attrs_to_retrieve = lappend_int(*attrs_to_retrieve, i);
		}
	}
	table_close(rel, NoLock);
}

/* 
 * Get a List of attribute name Strings from the given List of attribute numbers.
 */
static void get_attnames(TupleDesc tupdesc, List *attrs_to_retrieve,
			 List **attnames)
{
	ListCell *lc;
	FormData_pg_attribute *attr;
	char *attname;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	foreach(lc, attrs_to_retrieve) {
		int i = lfirst_int(lc);

		if (i > 0) {
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			attr = TupleDescAttr(tupdesc, i - 1);
			attname = NameStr(attr->attname);
			*attnames = lappend(*attnames,
					    makeString(pstrdup(attname)));
		}
	}
}

static HeapTuple make_tuple_from_result(jobject result, Relation rel,
					List *attrs_to_retrieve)
{
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	ListCell *lc;
	FormData_pg_attribute attr;
	char *attname;
	HeapTuple tuple;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	tupdesc = RelationGetDescr(rel);

	values = (Datum *)palloc0(tupdesc->natts * sizeof(Datum));
	nulls = (bool *)palloc(tupdesc->natts * sizeof(bool));

	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, tupdesc->natts * sizeof(bool));

	foreach(lc, attrs_to_retrieve) {
		int i = lfirst_int(lc);

		if (i > 0) {
			/* ordinary column */
			Assert(i <= tupdesc->natts);
			attr = tupdesc->attrs[i - 1];
			attname = NameStr(attr.attname);
			nulls[i - 1] = scalardb_result_is_null(result, attname);
			values[i - 1] = convert_result_column_to_datum(
				result, attname, attr.atttypid);
		}
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return tuple;
}

static Datum convert_result_column_to_datum(jobject result, char *attname,
					    Oid atttypid)
{
	ereport(DEBUG5, errmsg("entering function %s", __func__));
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
		text *val = scalardb_result_get_text(result, attname);
		PG_RETURN_TEXT_P(val);
	}
	case BYTEAOID: {
		bytea *val = scalardb_result_get_blob(result, attname);
		PG_RETURN_BYTEA_P(val);
	}
	default:
		ereport(ERROR, errmsg("Unsupported data type: %d", atttypid));
	}
}

/* 
 * Prepare the scan conditions for the ScalarDB Scan by evaluating the fdw_expr.
 */
static ScalarDbFdwScanCondition *
prepare_scan_conds(ForeignScanState *node, List *fdw_expr, List *key_names,
		   List *key_types, List *operators)
{
	size_t len;
	ScalarDbFdwScanCondition *scan_conds;

	List *fdw_expr_states = NIL;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int i;
	ListCell *lc;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	len = list_length(fdw_expr);
	scan_conds = palloc0(sizeof(ScalarDbFdwScanCondition) * len);

	fdw_expr_states = ExecInitExprList(fdw_expr, (PlanState *)node);

	i = 0;
	foreach(lc, fdw_expr) {
		Expr *expr = (Expr *)lfirst(lc);
		ExprState *expr_state =
			(ExprState *)list_nth(fdw_expr_states, i);
		char *name = strVal(list_nth(key_names, i));
		ScalarDbFdwConditionKeyType condition_key_type =
			list_nth_int(key_types, i);
		ScalarDbFdwConditionOperator condition_operator =
			list_nth_int(operators, i);

		Datum expr_value;
		bool isNull;

		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		scan_conds[i].key = condition_key_type;
		scan_conds[i].op = condition_operator;
		scan_conds[i].name = name;
		scan_conds[i].value = isNull ? (Datum)NULL : expr_value;
		scan_conds[i].value_type = exprType((Node *)expr);
		i++;
	}
	return scan_conds;
}

static List *exprs_to_strings(ScalarDbFdwScanCondition *scan_conds,
			      size_t scan_conds_len)
{
	List *strs = NIL;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	for (int i = 0; i < scan_conds_len; i++) {
		ScalarDbFdwScanCondition *scan_cond = &scan_conds[i];

		char *op_str;
		char *value_str;

		Oid typefnoid;
		bool isvarlena;
		FmgrInfo flinfo;

		Assert(value != NULL);

		switch (scan_cond->op) {
		case SCALARDB_OP_EQ:
			op_str = "=";
			break;
		case SCALARDB_OP_LE:
			op_str = "<=";
			break;
		case SCALARDB_OP_LT:
			op_str = "<";
			break;
		case SCALARDB_OP_GE:
			op_str = ">=";
			break;
		case SCALARDB_OP_GT:
			op_str = ">";
			break;
		}

		if (scan_cond->value_type == BOOLOID) {
			value_str = DatumGetBool(scan_cond->value) ? "true" :
								     "false";
		} else {
			getTypeOutputInfo(scan_cond->value_type, &typefnoid,
					  &isvarlena);
			fmgr_info(typefnoid, &flinfo);
			value_str =
				OutputFunctionCall(&flinfo, scan_cond->value);
		}

		strs = lappend(strs,
			       makeString(psprintf("%s %s %s", scan_cond->name,
						   op_str, value_str)));
	}
	return strs;
}

static ScalarDbFdwScanType determine_scan_type(List *condition_types)
{
	ListCell *lc;
	foreach(lc, condition_types) {
		switch ((ScalarDbFdwConditionKeyType)lfirst_int(lc)) {
		case SCALARDB_PARTITION_KEY:
			return SCALARDB_FDW_SCAN_PARTITION_KEY;
		case SCALARDB_SECONDARY_INDEX:
			return SCALARDB_FDW_SCAN_SECONDARY_INDEX;
		case SCALARDB_CLUSTERING_KEY:
			continue;
		}
	}
	return SCALARDB_FDW_SCAN_ALL;
}
