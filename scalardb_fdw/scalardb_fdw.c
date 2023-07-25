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
#include "column_metadata.h"
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

	/* Conditions on the index keys or secondary indexes that are pushed to ScalarDB side */
	List *remote_conds;
	/* Conditions that are evaluated locally */
	List *local_conds;
	/* the clustering keys that are pushed to ScalarDB side */
	ScalarDbFdwClusteringKeyBoundary boundary;
	/* Type of Scan executed on the ScalarDB side. This must be consistent with the condtitions in remote_conds */
	ScalarDbFdwScanType scan_type;

	/* estimate of physical size */
	BlockNumber pages;
	/* estimate of number of data rows */
	double tuples;

	/* set of the column metadata of the table*/
	ScalarDbFdwColumnMetadata column_metadata;
} ScalarDbFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct {
	ScalarDbFdwOptions options;

	/* extracted fdw_private data. See the following enum for the content*/
	List *attrs_to_retrieve;
	List *condition_key_names;
	ScalarDbFdwScanType scan_type;
	List *boundary_column_names;
	List *boundary_is_equals;
	int boundary_start_expr_offset;
	int boundary_start_inclusive;
	int boundary_end_expr_offset;
	int boundary_end_inclusive;

	/* List of retrieved attribute names, coverted from attrs_to_retrieve */
	List *attnames;
	/* relcache entry for the foreign table */
	Relation rel;
	/* attribute datatype conversion metadata */
	AttInMetadata *attinmeta;

	/* Array of Conditions for ScalarDB Scan */
	ScalarDbFdwScanCondition *scan_conds;
	/* number of conditions in scan_conds */
	size_t num_scan_conds;
	/* Clusteirng key boundary for ScalarDB Scan */
	ScalarDbFdwScanBoundary *boundary;

	/* Java instance of com.scalar.db.api.Scan.*/
	jobject scan;
	/* Java instance of com.scalar.db.api.Scanner */
	jobject scanner;
} ScalarDbFdwScanState;

enum ScanFdwPrivateIndex {
	/* Integer list of attribute numbers retrieved by the SELECT */
	ScanFdwPrivateAttrsToRetrieve,
	/* List of String that contains column names of the pushed-down key conditions */
	ScanFdwPrivateConditionColumnNames,
	/* Type of Scan executed on the ScalarDB side. */
	ScanFdwPrivateScanType,
	/* List of String that contains columns names of the clustring key bounds */
	ScanFdwPrivateBoundaryColumnNames,
	/* List of Boolean that indicates whether each condition is equal operation */
	ScanFdwPrivateBoundaryIsEquals,
	/* Index offset in fdw_exprs where the expressions for start boundary starts */
	ScanFdwPrivateBoundaryStartExprOffset,
	/* Boolean indiates whether start condition is inclusive */
	ScanFdwPrivateBoundaryStartInclusive,
	/* Index offset in fdw_exprs where the expressions for end boundary starts */
	ScanFdwPrivateBoundaryEndExprOffset,
	/* Boolean indiates whether end condition is inclusive */
	ScanFdwPrivateBoundaryEndInclusive,
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
prepare_scan_conds(ExprContext *econtext, List *fdw_expr, List *fdw_expr_states,
		   List *key_names, size_t num_scan_conds);
static ScalarDbFdwScanBoundary *prepare_scan_boundary(
	ExprContext *econtext, List *fdw_expr, List *fdw_expr_states,
	List *column_names, size_t start_expr_offset, bool start_inclusive,
	size_t end_expr_offset, bool end_inclusive, List *is_equals);

static char *scan_conds_to_string(ScalarDbFdwScanCondition *scan_conds,
				  size_t num_conds);
static char *scan_start_boundary_to_string(ScalarDbFdwScanBoundary *boundary);
static char *scan_end_boundary_to_string(ScalarDbFdwScanBoundary *boundary);

/*
 * FDW callback routines
 */
static void scalardbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
				      Oid foreigntableid);

static void scalardbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
				    Oid foreigntableid);

static ForeignScan *
scalardbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
		       Oid foreigntableid, ForeignPath *best_path, List *tlist,
		       List *scan_clauses, Plan *outer_plan);

static void scalardbBeginForeignScan(ForeignScanState *node, int eflags);

static TupleTableSlot *scalardbIterateForeignScan(ForeignScanState *node);

static void scalardbReScanForeignScan(ForeignScanState *node);

static void scalardbEndForeignScan(ForeignScanState *node);

static void scalardbExplainForeignScan(ForeignScanState *node,
				       ExplainState *es);

static bool scalardbAnalyzeForeignTable(Relation relation,
					AcquireSampleRowsFunc *func,
					BlockNumber *totalpages);

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
	 * Separate baserestrictinfo into three groups:
	 * 1. remote_conds: conditions that will be pushed down to ScalarDB
	 * 2. local_conds: conditions that will be evaluated locally
	 * 3. boundary: conditions that will be pushed down to ScalarDB to used to determine
	 *              clustering key boudnary.
	 */
	determine_remote_conds(baserel, baserel->baserestrictinfo,
			       &fdw_private->column_metadata,
			       &fdw_private->remote_conds,
			       &fdw_private->local_conds,
			       &fdw_private->boundary, &fdw_private->scan_type);

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

	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *fdw_recheck_quals = NIL;

	List *fdw_private_for_scan = NIL;

	List *fdw_exprs = NIL;
	List *condition_key_names = NIL;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	fdw_private = (ScalarDbFdwPlanState *)baserel->fdw_private;

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
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local
	 * execution.
	 */
	foreach(lc, scan_clauses) {
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		Var *left;
		String *left_name;
		Expr *right;

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fdw_private->remote_conds, rinfo)) {
			remote_exprs = lappend(remote_exprs, rinfo->clause);

			split_condition_expr(baserel,
					     &fdw_private->column_metadata,
					     rinfo->clause, &left, &left_name,
					     &right);
			fdw_exprs = lappend(fdw_exprs, right);
			condition_key_names =
				lappend(condition_key_names, left_name);
		} else if (list_member_ptr(fdw_private->boundary.conds,
					   rinfo)) {
			remote_exprs = lappend(remote_exprs, rinfo->clause);
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

	fdw_private_for_scan = list_make3(attrs_to_retrieve,
					  condition_key_names,
					  makeInteger(fdw_private->scan_type));

	/* 
	 * Put information on the clustering key boudnary
	*/
	fdw_private_for_scan =
		lappend(fdw_private_for_scan, fdw_private->boundary.names);
	fdw_private_for_scan =
		lappend(fdw_private_for_scan, fdw_private->boundary.is_equals);

	/* Start boundary */
	fdw_private_for_scan = lappend(fdw_private_for_scan,
				       makeInteger(list_length(fdw_exprs)));
	fdw_exprs = list_concat(fdw_exprs, fdw_private->boundary.start_exprs);
	fdw_private_for_scan =
		lappend(fdw_private_for_scan,
			makeBoolean(fdw_private->boundary.start_inclusive));

	/* End boudnary */
	fdw_private_for_scan = lappend(fdw_private_for_scan,
				       makeInteger(list_length(fdw_exprs)));
	fdw_exprs = list_concat(fdw_exprs, fdw_private->boundary.end_exprs);
	fdw_private_for_scan =
		lappend(fdw_private_for_scan,
			makeBoolean(fdw_private->boundary.end_inclusive));

	return make_foreignscan(
		tlist, local_exprs,
		baserel->relid, /* For base relations, set scan_relid as the relid of the relation. */
		fdw_exprs, fdw_private_for_scan, /* private state */
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
	List *fdw_expr_states;

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

	fdw_state->scan_type =
		intVal(list_nth(fsplan->fdw_private, ScanFdwPrivateScanType));

	fdw_state->boundary_column_names = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryColumnNames);

	fdw_state->boundary_is_equals = (List *)list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryIsEquals);

	fdw_state->boundary_start_expr_offset = intVal(list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryStartExprOffset));

	fdw_state->boundary_start_inclusive = boolVal(list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryStartInclusive));

	fdw_state->boundary_end_expr_offset = intVal(list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryEndExprOffset));

	fdw_state->boundary_end_inclusive = boolVal(list_nth(
		fsplan->fdw_private, ScanFdwPrivateBoundaryEndInclusive));

	/* Get info we'll need for input data conversion. */
	fdw_state->rel = node->ss.ss_currentRelation;
	fdw_state->attinmeta =
		TupleDescGetAttInMetadata(RelationGetDescr(fdw_state->rel));

	get_attnames(fdw_state->attinmeta->tupdesc,
		     fdw_state->attrs_to_retrieve, &fdw_state->attnames);

	/* Prepare conditions for Scan */
	fdw_expr_states =
		ExecInitExprList(fsplan->fdw_exprs, (PlanState *)node);
	fdw_state->num_scan_conds = fdw_state->boundary_start_expr_offset;
	fdw_state->scan_conds = prepare_scan_conds(
		node->ss.ps.ps_ExprContext, fsplan->fdw_exprs, fdw_expr_states,
		fdw_state->condition_key_names, fdw_state->num_scan_conds);

	/* Instanciate Scan object of ScalarDb*/
	switch (fdw_state->scan_type) {
	case SCALARDB_SCAN_ALL:
		fdw_state->scan = scalardb_scan_all(
			fdw_state->options.namespace,
			fdw_state->options.table_name, fdw_state->attnames);
		break;
	case SCALARDB_SCAN_PARTITION_KEY: {
		fdw_state->boundary = prepare_scan_boundary(
			node->ss.ps.ps_ExprContext, fsplan->fdw_exprs,
			fdw_expr_states, fdw_state->boundary_column_names,
			fdw_state->boundary_start_expr_offset,
			fdw_state->boundary_start_inclusive,
			fdw_state->boundary_end_expr_offset,
			fdw_state->boundary_end_inclusive,
			fdw_state->boundary_is_equals);
		fdw_state->scan = scalardb_scan(fdw_state->options.namespace,
						fdw_state->options.table_name,
						fdw_state->attnames,
						fdw_state->scan_conds,
						fdw_state->num_scan_conds,
						fdw_state->boundary);
		break;
	}
	case SCALARDB_SCAN_SECONDARY_INDEX:
		fdw_state->scan = scalardb_scan_with_index(
			fdw_state->options.namespace,
			fdw_state->options.table_name, fdw_state->attnames,
			fdw_state->scan_conds, fdw_state->num_scan_conds);
		break;
	}

	ereport(DEBUG5, errmsg("ScalarDB Scan %s",
			       scalardb_to_string(fdw_state->scan)));

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

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	fdw_state = (ScalarDbFdwScanState *)node->fdw_state;
	ExplainPropertyText("ScalarDB Namespace", fdw_state->options.namespace,
			    es);
	ExplainPropertyText("ScalarDB Table", fdw_state->options.table_name,
			    es);
	if (es->verbose) {
		char *scan_type_str = NULL;
		switch (fdw_state->scan_type) {
		case SCALARDB_SCAN_ALL:
			scan_type_str = "all";
			break;
		case SCALARDB_SCAN_PARTITION_KEY:
			scan_type_str = "partition key";
			break;
		case SCALARDB_SCAN_SECONDARY_INDEX:
			scan_type_str = "secondary index";
			break;
		}

		ExplainPropertyText("ScalarDB Scan Type", scan_type_str, es);

		if (fdw_state->num_scan_conds > 0) {
			char *scan_conds_str =
				scan_conds_to_string(fdw_state->scan_conds,
						     fdw_state->num_scan_conds);

			ExplainPropertyText("ScalarDB Scan Condition",
					    scan_conds_str, es);
		}

		if (fdw_state->boundary != NULL) {
			if (fdw_state->boundary->num_start_values > 0) {
				char *start_boundary_str =
					scan_start_boundary_to_string(
						fdw_state->boundary);
				ExplainPropertyText("ScalarDB Scan Start",
						    start_boundary_str, es);
			}

			if (fdw_state->boundary->num_end_values > 0) {
				char *end_boundary_str =
					scan_end_boundary_to_string(
						fdw_state->boundary);
				ExplainPropertyText("ScalarDB Scan End",
						    end_boundary_str, es);
			}
		}

		if (list_length(fdw_state->attnames) > 0)
			ExplainPropertyText("ScalarDB Scan Attribute",
					    nodeToString(fdw_state->attnames),
					    es);
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
static ScalarDbFdwScanCondition *prepare_scan_conds(ExprContext *econtext,
						    List *fdw_exprs,
						    List *fdw_expr_states,
						    List *key_names,
						    size_t num_scan_conds)
{
	ScalarDbFdwScanCondition *scan_conds;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	scan_conds = palloc0(sizeof(ScalarDbFdwScanCondition) * num_scan_conds);

	for (size_t i = 0; i < num_scan_conds; i++) {
		Expr *expr = list_nth(fdw_exprs, i);
		ExprState *expr_state =
			(ExprState *)list_nth(fdw_expr_states, i);
		char *name = strVal(list_nth(key_names, i));

		Datum expr_value;
		bool isNull;

		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

		scan_conds[i].name = name;
		scan_conds[i].value = isNull ? (Datum)NULL : expr_value;
		scan_conds[i].value_type = exprType((Node *)expr);
	}
	return scan_conds;
}

/* 
 * Prepare the clustering key boundary for the ScalarDB Scan by evaluating the fdw_expr.
 */
static ScalarDbFdwScanBoundary *prepare_scan_boundary(
	ExprContext *econtext, List *fdw_exprs, List *fdw_expr_states,
	List *column_names, size_t start_expr_offset, bool start_inclusive,
	size_t end_expr_offset, bool end_inclusive, List *is_equals)
{
	ScalarDbFdwScanBoundary *boundary;

	ereport(DEBUG5, errmsg("entering function %s", __func__));

	boundary = palloc0(sizeof(ScalarDbFdwScanBoundary));

	boundary->names = column_names;
	boundary->start_inclusive = start_inclusive;
	boundary->end_inclusive = end_inclusive;
	boundary->is_equals = is_equals;

	boundary->num_start_values = end_expr_offset - start_expr_offset;
	boundary->start_values =
		palloc0(sizeof(Datum) * boundary->num_start_values);
	for (size_t i = start_expr_offset; i < end_expr_offset; i++) {
		Expr *expr = list_nth(fdw_exprs, i);
		ExprState *expr_state =
			(ExprState *)list_nth(fdw_expr_states, i);

		Datum expr_value;
		bool isNull;

		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
		boundary->start_values[i - start_expr_offset] =
			isNull ? (Datum)NULL : expr_value;
		boundary->start_value_types = lappend_oid(
			boundary->start_value_types, exprType((Node *)expr));
	}

	boundary->num_end_values = list_length(fdw_exprs) - end_expr_offset;
	boundary->end_values =
		palloc0(sizeof(Datum) * boundary->num_end_values);
	for (size_t i = end_expr_offset; i < list_length(fdw_exprs); i++) {
		Expr *expr = list_nth(fdw_exprs, i);
		ExprState *expr_state =
			(ExprState *)list_nth(fdw_expr_states, i);

		Datum expr_value;
		bool isNull;

		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
		boundary->end_values[i - end_expr_offset] =
			isNull ? (Datum)NULL : expr_value;
		boundary->end_value_types = lappend_oid(
			boundary->end_value_types, exprType((Node *)expr));
	}

	return boundary;
}

static char *scan_conds_to_string(ScalarDbFdwScanCondition *scan_conds,
				  size_t num_scan_conds)
{
	StringInfoData str;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	initStringInfo(&str);

	for (int i = 0; i < num_scan_conds; i++) {
		ScalarDbFdwScanCondition *scan_cond = &scan_conds[i];

		Oid typefnoid;
		bool isvarlena;
		FmgrInfo flinfo;

		if (i > 0) {
			appendStringInfoString(&str, " AND ");
		}

		appendStringInfo(&str, "%s = ", scan_cond->name);

		if (scan_cond->value_type == BOOLOID) {
			appendStringInfoString(
				&str, DatumGetBool(scan_cond->value) ? "true" :
								       "false");
		} else {
			getTypeOutputInfo(scan_cond->value_type, &typefnoid,
					  &isvarlena);
			fmgr_info(typefnoid, &flinfo);

			if (scan_cond->value_type == TEXTOID)
				appendStringInfoChar(&str, '\'');
			else if (scan_cond->value_type == BYTEAOID)
				appendStringInfoString(&str, "E'\\");

			appendStringInfoString(
				&str,
				OutputFunctionCall(&flinfo, scan_cond->value));
			if (scan_cond->value_type == TEXTOID ||
			    scan_cond->value_type == BYTEAOID)
				appendStringInfoChar(&str, '\'');
		}
	}
	return str.data;
}

static char *scan_start_boundary_to_string(ScalarDbFdwScanBoundary *boundary)
{
	StringInfoData str;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	initStringInfo(&str);
	for (int i = 0; i < boundary->num_start_values; i++) {
		Datum value = boundary->start_values[i];
		Oid value_type = list_nth_oid(boundary->start_value_types, i);
		char *name = strVal(list_nth(boundary->names, i));

		Oid typefnoid;
		bool isvarlena;
		FmgrInfo flinfo;

		if (i > 0) {
			appendStringInfoString(&str, " AND ");
		}

		appendStringInfoString(&str, name);

		if (boolVal(list_nth(boundary->is_equals, i))) {
			appendStringInfoString(&str, " = ");
		} else if (boundary->start_inclusive) {
			appendStringInfoString(&str, " >= ");
		} else {
			appendStringInfoString(&str, " > ");
		}

		if (value_type == BOOLOID) {
			appendStringInfoString(
				&str, DatumGetBool(value) ? "true" : "false");
		} else {
			getTypeOutputInfo(value_type, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &flinfo);

			if (value_type == TEXTOID)
				appendStringInfoChar(&str, '\'');
			else if (value_type == BYTEAOID)
				appendStringInfoString(&str, "E'\\");

			appendStringInfoString(&str, OutputFunctionCall(&flinfo,
									value));

			if (value_type == TEXTOID || value_type == BYTEAOID)
				appendStringInfoChar(&str, '\'');
		}
	}
	return str.data;
}

static char *scan_end_boundary_to_string(ScalarDbFdwScanBoundary *boundary)
{
	StringInfoData str;

	ereport(DEBUG4, errmsg("entering function %s", __func__));

	initStringInfo(&str);
	for (int i = 0; i < boundary->num_end_values; i++) {
		Datum value = boundary->end_values[i];
		Oid value_type = list_nth_oid(boundary->end_value_types, i);
		char *name = strVal(list_nth(boundary->names, i));

		Oid typefnoid;
		bool isvarlena;
		FmgrInfo flinfo;

		if (i > 0) {
			appendStringInfoString(&str, " AND ");
		}

		appendStringInfoString(&str, name);

		if (boolVal(list_nth(boundary->is_equals, i))) {
			appendStringInfoString(&str, " = ");
		} else if (boundary->end_inclusive) {
			appendStringInfoString(&str, " <= ");
		} else {
			appendStringInfoString(&str, " < ");
		}

		if (value_type == BOOLOID) {
			appendStringInfoString(
				&str, DatumGetBool(value) ? "true" : "false");
		} else {
			getTypeOutputInfo(value_type, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &flinfo);

			if (value_type == TEXTOID)
				appendStringInfoChar(&str, '\'');
			else if (value_type == BYTEAOID)
				appendStringInfoString(&str, "E'\\");

			appendStringInfoString(&str, OutputFunctionCall(&flinfo,
									value));

			if (value_type == TEXTOID || value_type == BYTEAOID)
				appendStringInfoChar(&str, '\'');
		}
	}
	return str.data;
}
