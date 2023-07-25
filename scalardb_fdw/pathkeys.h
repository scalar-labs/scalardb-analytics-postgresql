#ifndef SCALARDB_FDW_CONDITION_H
#define SCALARDB_FDW_CONDITION_H

#include "c.h"
#include "postgres.h"
#include "optimizer/pathnode.h"

#include "column_metadata.h"

extern void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
				ScalarDbFdwColumnMetadata *column_metadata);

#endif
