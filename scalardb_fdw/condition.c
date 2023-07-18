#include <string.h>

#include "c.h"
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "nodes/primnodes.h"
#include "nodes/pg_list.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "utils/syscache.h"

#include "condition.h"
#include "scalardb_fdw_util.h"

#define UnsupportedConitionType -1

/*
 * Type of the condition target column
 */
typedef enum {
	SCALARDB_PARTITION_KEY,
	SCALARDB_CLUSTERING_KEY,
	SCALARDB_SECONDARY_INDEX,
} ScalarDbFdwConditionKeyType;

typedef enum {
	SCALARDB_OP_EQ,
	SCALARDB_OP_LE,
	SCALARDB_OP_LT,
	SCALARDB_OP_GE,
	SCALARDB_OP_GT,
} ScalarDbFdwOperator;

/*
 * Represents a shippable condition in the planner phase.
 */
typedef struct {
	/*  type of condition. See the above definition. */
	ScalarDbFdwConditionKeyType key;
	/* key index in the partition key, clustering key, or secondary index */
	int key_index;
	/*  type of operator. */
	ScalarDbFdwOperator op;
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

static void determine_clustering_key_boundary(
	List *clustering_key_conds, List *clustering_key_shippable_conds,
	int num_clustering_keys, ScalarDbFdwClusteringKeyBoundary *boundary);

static ScalarDbFdwShippableCondition *
is_shippable_condition(RelOptInfo *baserel,
		       ScalarDbFdwColumnMetadata *column_metadata, Expr *expr);

static bool is_foreign_table_var(Var *var, RelOptInfo *baserel);

static ScalarDbFdwOperator get_operator_type(OpExpr *op);

static Const *make_boolean_const(bool val);

/*
 * Determine which conditions can be pushed down to the foreign server (referred to as shippable).
 *
 * remote_conds must contain one of the partition key or secondary index conditions
 * These conditions must be equal operations.
 *
 * The number of partition key conditions must be equal to the number of partition keys to be appended to remote_conds.
 * In other words, all partition keys must be specified in WHERE clause[].
 *
 * If remote_conds contain the partition key conditions, remote_conds may contain zero or more the clustring key conditions.
 *
 * If input_conds contains multiple secondary index condtiions, only the first condition found is appended to remote_conds.
 */
extern void determine_remote_conds(RelOptInfo *baserel, List *input_conds,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **remote_conds, List **local_conds,
				   ScalarDbFdwClusteringKeyBoundary *boundary,
				   ScalarDbFdwScanType *scan_type)
{
	ListCell *lc;
	ScalarDbFdwShippableCondition *shippable_condition;
	List *partition_key_conds = NIL;
	List *clustering_key_conds = NIL;
	List *clustering_key_shippable_conds = NIL;
	RestrictInfo *secondary_index_cond = NULL;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	foreach(lc, input_conds) {
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		shippable_condition = is_shippable_condition(
			baserel, column_metadata, ri->clause);
		if (shippable_condition != NULL) {
			switch (shippable_condition->key) {
			case SCALARDB_PARTITION_KEY: {
				partition_key_conds =
					lappend(partition_key_conds, ri);
				pfree(shippable_condition);
				break;
			}
			case SCALARDB_CLUSTERING_KEY: {
				clustering_key_conds =
					lappend(clustering_key_conds, ri);
				/* Not that the type of the element is ScalarDbFdwShippableCondition here.
				 * This type is not a serilizable Node of List, but it would not be problematic
				 * because this is used only with in this function */
				clustering_key_shippable_conds =
					lappend(clustering_key_shippable_conds,
						shippable_condition);
				break;
			}
			case SCALARDB_SECONDARY_INDEX: {
				/* Use only the first condition on the secondary index */
				if (secondary_index_cond == NULL) {
					secondary_index_cond = ri;
				}
				pfree(shippable_condition);
				break;
			}
			}
		}
	}

	if (list_length(partition_key_conds) ==
	    list_length(column_metadata->partition_key_attnums)) {
		*scan_type = SCALARDB_SCAN_PARTITION_KEY;
		*remote_conds = partition_key_conds;
		*local_conds = list_difference(input_conds, *remote_conds);

		determine_clustering_key_boundary(
			clustering_key_conds, clustering_key_shippable_conds,
			list_length(column_metadata->clustering_key_attnums),
			boundary);

		if (list_length(boundary->conds) > 0) {
			*local_conds =
				list_difference(*local_conds, boundary->conds);
		}
	} else if (secondary_index_cond != NULL) {
		*scan_type = SCALARDB_SCAN_SECONDARY_INDEX;
		*remote_conds = list_make1(secondary_index_cond);
		*local_conds = list_difference(input_conds, *remote_conds);
	} else {
		*scan_type = SCALARDB_SCAN_ALL;
		*remote_conds = NIL;
		*local_conds = input_conds;
	}
}

/*
 * Split the given condition expression the left Var that indicates the condition variable
 * and the right Expr that indicates the condition value.
 *
 * It is caller's responsibility to ensure that the given condition expression is shippable
 * by calling determine_remote_conds.
 */
extern void split_condition_expr(RelOptInfo *baserel,
				 ScalarDbFdwColumnMetadata *column_metadata,
				 Expr *expr, Var **left, String **left_name,
				 Expr **right)
{
	ScalarDbFdwShippableCondition *cond;

	cond = is_shippable_condition(baserel, column_metadata, expr);

	if (cond == NULL)
		ereport(ERROR, errmsg("condition is not shippable"));

	*left = cond->column;
	*left_name = cond->name;
	*right = cond->expr;
}

static void determine_clustering_key_boundary(
	List *clustering_key_conds, List *clustering_key_shippable_conds,
	int num_clustering_keys, ScalarDbFdwClusteringKeyBoundary *boundary)
{
	ListCell *lc, *lc2;
	bool start_boundary_is_set = false;
	bool end_boundary_is_set = false;

	/* Check for keys in order to determine the boundary 
	 * until the first non-EQ condition (i.e., range condition) is found */
	for (int i = 0; i < num_clustering_keys; i++) {
		forboth(lc, clustering_key_conds, lc2,
			clustering_key_shippable_conds)
		{
			RestrictInfo *ri = lfirst(lc);
			ScalarDbFdwShippableCondition *cond = lfirst(lc2);

			if (cond->key_index == i) {
				switch (cond->op) {
				case SCALARDB_OP_EQ:
					/* Skip if range condition was found eariler */
					if (start_boundary_is_set ||
					    end_boundary_is_set)
						continue;

					boundary->names = lappend(
						boundary->names, cond->name);
					boundary->start_exprs =
						lappend(boundary->start_exprs,
							cond->expr);
					boundary->end_exprs =
						lappend(boundary->end_exprs,
							cond->expr);
					boundary->conds =
						lappend(boundary->conds, ri);
					boundary->is_equals =
						lappend(boundary->is_equals,
							makeBoolean(true));

					boundary->start_inclusive = true;
					boundary->end_inclusive = true;

					/* Check the next clustering key */
					goto NEXT_CLUSTERING_KEY;
				case SCALARDB_OP_LE:
				case SCALARDB_OP_LT:
					/* Skip if end boundary is already set*/
					if (end_boundary_is_set)
						continue;

					boundary->end_exprs =
						lappend(boundary->end_exprs,
							cond->expr);
					boundary->end_inclusive =
						cond->op == SCALARDB_OP_LE;
					boundary->conds =
						lappend(boundary->conds, ri);

					if (start_boundary_is_set) {
						/* Stop the boundary check because
						 * both start and end for the current clustering key is set */
						goto FINISH;
					} else {
						boundary->names =
							lappend(boundary->names,
								cond->name);
						boundary->is_equals = lappend(
							boundary->is_equals,
							makeBoolean(false));
						end_boundary_is_set = true;
						/* Try to find start boundary condition */
						continue;
					}
				case SCALARDB_OP_GE:
				case SCALARDB_OP_GT:
					/* Skip if start boundary is already set*/
					if (start_boundary_is_set)
						continue;

					boundary->start_exprs =
						lappend(boundary->start_exprs,
							cond->expr);
					boundary->start_inclusive =
						cond->op == SCALARDB_OP_GE;
					boundary->conds =
						lappend(boundary->conds, ri);

					if (end_boundary_is_set) {
						/* Stop the boundary check because
						 * both start and end for the current clustering key is set */
						goto FINISH;
					} else {
						boundary->names =
							lappend(boundary->names,
								cond->name);
						boundary->is_equals = lappend(
							boundary->is_equals,
							makeBoolean(false));
						start_boundary_is_set = true;
						/* Try to find end boundary condition */
						continue;
					}
				}
			}
		}
		/* In the case that one of the start or end boudary is detected for the key;
		 * - if it is the first key in the list, use it as an one-side boundary
		 * - if the list already has other keys, use only them
		 *   (i.e., use only conditions of EQ operator) */
		if (start_boundary_is_set && !end_boundary_is_set) {
			if (list_length(boundary->start_exprs) > 1) {
				boundary->names =
					list_delete_last(boundary->names);
				boundary->is_equals =
					list_delete_last(boundary->is_equals);
				boundary->conds =
					list_delete_last(boundary->conds);
				boundary->start_exprs =
					list_delete_last(boundary->start_exprs);
				boundary->start_inclusive = true;
			}
		}
		if (end_boundary_is_set && !start_boundary_is_set) {
			if (list_length(boundary->end_exprs) > 1) {
				boundary->names =
					list_delete_last(boundary->names);
				boundary->is_equals =
					list_delete_last(boundary->is_equals);
				boundary->conds =
					list_delete_last(boundary->conds);
				boundary->end_exprs =
					list_delete_last(boundary->end_exprs);
				boundary->end_inclusive = true;
			}
		}

		/* Stop the boundary check if no condition is found for the current clustering key */
		goto FINISH;
NEXT_CLUSTERING_KEY:
		continue;
	}
FINISH:
	return;
}

static ScalarDbFdwShippableCondition *
check_keys_for_var(Var *var, List *key_attnums, List *key_names,
		   ScalarDbFdwConditionKeyType key_type, ScalarDbFdwOperator op,
		   Expr *expr)
{
	ListCell *lc;
	int i = 0;
	ScalarDbFdwShippableCondition *cond;

	foreach(lc, key_attnums) {
		int attnum = lfirst_int(lc);
		if (attnum == var->varattno) {
			cond = palloc0(sizeof(ScalarDbFdwShippableCondition));
			cond->key = key_type;
			cond->key_index = i;
			cond->op = op;
			cond->column = var;
			cond->name = list_nth(key_names, i);
			cond->expr = expr;
			return cond;
		}
		i++;
	}
	return NULL;
}

static ScalarDbFdwShippableCondition *
is_shippable_condition(RelOptInfo *baserel,
		       ScalarDbFdwColumnMetadata *column_metadata, Expr *expr)
{
	ScalarDbFdwShippableCondition *cond;
	switch (nodeTag(expr)) {
	case T_Var: {
		Var *var = (Var *)expr;

		if (var->vartype != BOOLOID)
			return false;

		if (!is_foreign_table_var(var, baserel))
			return false;

		cond = check_keys_for_var(
			var, column_metadata->partition_key_attnums,
			column_metadata->partition_key_names,
			SCALARDB_PARTITION_KEY, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(true));
		if (cond != NULL)
			return cond;

		cond = check_keys_for_var(
			var, column_metadata->clustering_key_attnums,
			column_metadata->clustering_key_names,
			SCALARDB_CLUSTERING_KEY, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(true));
		if (cond != NULL)
			return cond;

		cond = check_keys_for_var(
			var, column_metadata->secondary_index_attnums,
			column_metadata->secondary_index_names,
			SCALARDB_SECONDARY_INDEX, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(true));
		if (cond != NULL)
			return cond;

		return false;
	}
	case T_BoolExpr: {
		BoolExpr *bool_expr = (BoolExpr *)expr;
		Var *var;

		/* Consider only NOT operator */
		if (bool_expr->boolop != NOT_EXPR)
			return false;

		if (!IsA(linitial(bool_expr->args), Var))
			return false;

		var = linitial_node(Var, bool_expr->args);

		if (!is_foreign_table_var(var, baserel))
			return false;

		cond = check_keys_for_var(
			var, column_metadata->partition_key_attnums,
			column_metadata->partition_key_names,
			SCALARDB_PARTITION_KEY, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(false));
		if (cond != NULL)
			return cond;

		cond = check_keys_for_var(
			var, column_metadata->clustering_key_attnums,
			column_metadata->clustering_key_names,
			SCALARDB_CLUSTERING_KEY, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(false));
		if (cond != NULL)
			return cond;

		cond = check_keys_for_var(
			var, column_metadata->secondary_index_attnums,
			column_metadata->secondary_index_names,
			SCALARDB_SECONDARY_INDEX, SCALARDB_OP_EQ,
			(Expr *)make_boolean_const(false));
		if (cond != NULL)
			return cond;

		return false;
	}
	case T_OpExpr: {
		Var *left;
		Node *right;
		OpExpr *op = (OpExpr *)expr;
		ScalarDbFdwOperator op_type = get_operator_type(op);

		if (op_type == UnsupportedConitionType)
			return false;

		if (list_length(op->args) != 2)
			return false;

		if (!IsA(linitial(op->args), Var))
			return false;

		left = linitial_node(Var, op->args);
		right = lsecond(op->args);

		if (!is_foreign_table_var(left, baserel))
			return false;

		if (!is_pseudo_constant_clause(right))
			return false;

		if (op_type == SCALARDB_OP_EQ) {
			cond = check_keys_for_var(
				left, column_metadata->partition_key_attnums,
				column_metadata->partition_key_names,
				SCALARDB_PARTITION_KEY, SCALARDB_OP_EQ,
				(Expr *)right);
			if (cond != NULL)
				return cond;
		}
		if (op_type == SCALARDB_OP_EQ || op_type == SCALARDB_OP_LE ||
		    op_type == SCALARDB_OP_LT || op_type == SCALARDB_OP_GE ||
		    op_type == SCALARDB_OP_GT) {
			cond = check_keys_for_var(
				left, column_metadata->clustering_key_attnums,
				column_metadata->clustering_key_names,
				SCALARDB_CLUSTERING_KEY, op_type,
				(Expr *)right);
			if (cond != NULL)
				return cond;
		}
		if (op_type == SCALARDB_OP_EQ) {
			cond = check_keys_for_var(
				left, column_metadata->secondary_index_attnums,
				column_metadata->secondary_index_names,
				SCALARDB_SECONDARY_INDEX, SCALARDB_OP_EQ,
				(Expr *)right);
			if (cond != NULL)
				return cond;
		}
		return false;
	}
	default:
		return false;
	}
}

static bool is_foreign_table_var(Var *var, RelOptInfo *baserel)
{
	return bms_is_member(var->varno, baserel->relids) &&
	       var->varlevelsup == 0;
}

static ScalarDbFdwOperator get_operator_type(OpExpr *op)
{
	HeapTuple tuple;
	Form_pg_operator form;
	ScalarDbFdwOperator ret = UnsupportedConitionType;

	if (list_length(op->args) != 2)
		return ret;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", op->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);

	if (strcmp(NameStr(form->oprname), "=") == 0)
		ret = SCALARDB_OP_EQ;
	else if (strcmp(NameStr(form->oprname), "<=") == 0)
		ret = SCALARDB_OP_LE;
	else if (strcmp(NameStr(form->oprname), "<") == 0)
		ret = SCALARDB_OP_LT;
	else if (strcmp(NameStr(form->oprname), ">=") == 0)
		ret = SCALARDB_OP_GE;
	else if (strcmp(NameStr(form->oprname), ">") == 0)
		ret = SCALARDB_OP_GT;

	ReleaseSysCache(tuple);
	return ret;
}

static Const *make_boolean_const(bool val)
{
	return makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(val), false,
			 true);
}
