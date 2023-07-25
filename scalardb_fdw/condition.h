#ifndef CONDITION_H
#define CONDITION_H

#include "c.h"
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "utils/syscache.h"

#include "column_metadata.h"
#include "pgport.h"

/*
 * Type of Scan of ScalarDB
 */
typedef enum {
	SCALARDB_SCAN_PARTITION_KEY,
	SCALARDB_SCAN_SECONDARY_INDEX,
	SCALARDB_SCAN_ALL,
} ScalarDbFdwScanType;

/*
 * Represents a boundary of the clustering key in Scan operations
 */
typedef struct {
	List *names;

	List *start_exprs;
	bool start_inclusive;

	List *end_exprs;
	bool end_inclusive;

	/* List of RestrictInfo* that holds sources of start and end conditions */
	List *conds;

	/* List of Boolean that indicate whether each condition is equal operation */
	List *is_equals;
} ScalarDbFdwClusteringKeyBoundary;

extern void determine_remote_conds(RelOptInfo *baserel, List *input_conds,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **remote_conds, List **local_conds,
				   ScalarDbFdwClusteringKeyBoundary *boundary,
				   ScalarDbFdwScanType *scan_type);

extern void split_condition_expr(RelOptInfo *baserel,
				 ScalarDbFdwColumnMetadata *column_metadata,
				 Expr *expr, Var **left, String **left_name,
				 Expr **right);

extern bool is_foreign_table_var(Expr *expr, RelOptInfo *baserel);

#endif
