#ifndef SCALARDB_FDW_H
#define SCALARDB_FDW_H

#include "c.h"
#include "postgres.h"

#include "commands/explain.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"

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

#endif
