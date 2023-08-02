#include "c.h"
#include "postgres.h"

#include "optimizer/cost.h"
#include "optimizer/optimizer.h"

#include "cost.h"
#include "scalardb_fdw.h"

#define DEFAULT_ROWS_FOR_PARTITION_KEY_SCAN 10

/*
 * Estimate the size of a foreign table.
 *
 * The main result is returned in baserel->rows.
 */
void estimate_size(PlannerInfo *root, RelOptInfo *baserel)
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

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
void estimate_costs(PlannerInfo *root, RelOptInfo *baserel, List *remote_conds,
		    double *rows, Cost *startup_cost, Cost *total_cost)
{
	Cost run_cost = 0;
	Cost cpu_per_tuple;
	ScalarDbFdwPlanState *fdw_private =
		(ScalarDbFdwPlanState *)baserel->fdw_private;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	*rows = list_length(fdw_private->remote_conds) > 0 ?
			DEFAULT_ROWS_FOR_PARTITION_KEY_SCAN :
			baserel->rows;

	*startup_cost = 0;
	*startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	/* Add in tlist eval cost for each output row */
	*startup_cost += baserel->reltarget->cost.startup;
	run_cost += baserel->reltarget->cost.per_tuple * *rows;

	*total_cost = *startup_cost + run_cost;
}
