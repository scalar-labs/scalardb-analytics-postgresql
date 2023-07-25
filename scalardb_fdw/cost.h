#ifndef SCALARDB_FDW_COST_H
#define SCALARDB_FDW_COST_H

#include "c.h"
#include "postgres.h"
#include "optimizer/pathnode.h"

extern void estimate_size(PlannerInfo *root, RelOptInfo *baserel);

extern void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   List *remote_conds, Cost *startup_cost,
			   Cost *total_cost);

#endif
