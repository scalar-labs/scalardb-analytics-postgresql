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

#include "scalardb_fdw_util.h"

/*
 * Type of shippable condition
 */
typedef enum {
	/* - partition key = pseudo constant */
	SCALARDB_PARTITION_KEY_EQ,
	/* - clustering key <= pseudo constant */
	SCALARDB_CLUSTERING_KEY_LE,
	/* - clustering key > pseudo constant */
	SCALARDB_CLUSTERING_KEY_GT,
	/* - secondary index = pseudo constant */
	SCALARDB_SECONDARY_INDEX_EQ
} ScalarDbFdwConditionType;

/*
 * Represents a shippable condition in the planner phase.
 */
typedef struct {
	/*  type of condition. See the above definition. */
	ScalarDbFdwConditionType condtition_type;
	/* Target column of the condition */
	Var *column;
	/* Name  of column*/
#if PG_VERSION_NUM >= 150000
	String *name;
#else
	Value *name;
#endif
	/* Expression that is compared with the column.
	 * This expression will be evaluated in the executor phase.
	 * This must be a pseudo constant */
	Expr *expr;
} ScalarDbFdwShippableCondition;

/*
 * Represents a Scan condition in the executor phase.
 */
typedef struct {
	/*  type of condition. See the above definition. */
	ScalarDbFdwConditionType condition_type;
	/* column name */
	char *name;
	/* condition value*/
	Datum value;
	/* type of value */
	Oid value_type;
} ScalarDbFdwScanCondition;

extern void determine_remote_conds(RelOptInfo *baserel, List *input_conds,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **remote_conds, List **local_conds);

extern bool
is_shippable_condition(RelOptInfo *baserel,
		       ScalarDbFdwColumnMetadata *column_metadata, Expr *expr,
		       ScalarDbFdwShippableCondition *shippable_condition);

#endif
