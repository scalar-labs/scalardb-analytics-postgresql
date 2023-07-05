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

typedef enum {
	SCALARDB_OP_EQ, /* = */
	SCALARDB_OP_LE, /* <= */
	SCALARDB_OP_GT, /* > */
	SCALARDB_OP_OTHER, /* operators cannot be shipped */
} ScalarDbFdwOperator;

static bool is_foreign_table_var(Var *var, RelOptInfo *baserel);

static ScalarDbFdwOperator get_operator_type(OpExpr *op);

static Const *make_boolean_const(bool val);

/*
 * Determine which conditions can be pushed down to the foreign server (referred to as shippable).
 *
 * Shippable conditions are in the following forms (in order of priority):
 * - partition key = pseudo constant AND clustering key <= pseudo constant AND clustering key > pseudo constant
 * - partition key = pseudo constant AND clustering key <= pseudo constant (= can be replaced with >)
 * - partition key = pseudo constant
 * - secondary index = pseudo constant
 *
 * Note that, if table has multiple parition keys, all partition keys must be specified in the condition to be pushed down.
 */
extern void determine_remote_conds(RelOptInfo *baserel, List *input_conds,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **remote_conds, List **local_conds)
{
	ListCell *lc;
	ScalarDbFdwShippableCondition shippable_condition;
	List *partition_key_conds = NIL;
	List *clustering_key_conds = NIL;
	RestrictInfo *secondary_index_cond = NULL;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	foreach(lc, input_conds) {
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
		if (is_shippable_condition(baserel, column_metadata, ri->clause,
					   &shippable_condition)) {
			switch (shippable_condition.condtition_type) {
			case SCALARDB_PARTITION_KEY_EQ: {
				partition_key_conds =
					lappend(partition_key_conds, ri);
				break;
			}
			case SCALARDB_CLUSTERING_KEY_LE:
			case SCALARDB_CLUSTERING_KEY_GT: {
				clustering_key_conds =
					lappend(clustering_key_conds, ri);
				break;
			}
			case SCALARDB_SECONDARY_INDEX_EQ: {
				if (secondary_index_cond == NULL) {
					secondary_index_cond = ri;
				}
				break;
			}
			}
		}
	}
	if (list_length(partition_key_conds) ==
	    list_length(column_metadata->partition_key_attnums)) {
		/* TODO: support push down clustering key conditions */
		*remote_conds = partition_key_conds;
		*local_conds = list_difference(input_conds, *remote_conds);
	} else if (secondary_index_cond != NULL) {
		*remote_conds = list_make1(secondary_index_cond);
		*local_conds = list_difference(input_conds, *remote_conds);
	} else {
		*remote_conds = NIL;
		*local_conds = input_conds;
	}
}

extern bool
is_shippable_condition(RelOptInfo *baserel,
		       ScalarDbFdwColumnMetadata *column_metadata, Expr *expr,
		       ScalarDbFdwShippableCondition *shippable_condition)
{
	ereport(DEBUG3, errmsg("clase %s", nodeToString(expr)));
	switch (nodeTag(expr)) {
	case T_Var: {
		Var *var = (Var *)expr;
		ListCell *lc;
		int i;

		if (var->vartype != BOOLOID)
			return false;

		if (!is_foreign_table_var(var, baserel))
			return false;

		i = 0;
		foreach(lc, column_metadata->partition_key_attnums) {
			int attnum = lfirst_int(lc);
			if (attnum == var->varattno) {
				shippable_condition->condtition_type =
					SCALARDB_PARTITION_KEY_EQ;
				shippable_condition->column = var;
				shippable_condition->name = list_nth(
					column_metadata->partition_key_names,
					i);
				shippable_condition->expr =
					(Expr *)make_boolean_const(true);
				return true;
			}
			i++;
		}

		i = 0;
		foreach(lc, column_metadata->secondary_index_attnums) {
			int attnum = lfirst_int(lc);
			if (attnum == var->varattno) {
				shippable_condition->condtition_type =
					SCALARDB_SECONDARY_INDEX_EQ;
				shippable_condition->column = var;
				shippable_condition->name = list_nth(
					column_metadata->secondary_index_names,
					i);
				shippable_condition->expr =
					(Expr *)make_boolean_const(true);
				return true;
			}
			i++;
		}
		return false;
	}
	case T_BoolExpr: {
		BoolExpr *bool_expr = (BoolExpr *)expr;
		Var *var;
		ListCell *lc;
		int i;

		/* Consier only NOT operator */
		if (bool_expr->boolop != NOT_EXPR)
			return false;

		if (!IsA(linitial(bool_expr->args), Var))
			return false;

		var = linitial_node(Var, bool_expr->args);

		if (!is_foreign_table_var(var, baserel))
			return false;

		i = 0;
		foreach(lc, column_metadata->partition_key_attnums) {
			int attnum = lfirst_int(lc);
			if (attnum == var->varattno) {
				shippable_condition->condtition_type =
					SCALARDB_PARTITION_KEY_EQ;
				shippable_condition->column = var;
				shippable_condition->name = list_nth(
					column_metadata->partition_key_names,
					i);
				shippable_condition->expr =
					(Expr *)make_boolean_const(false);
				return true;
			}
			i++;
		}

		i = 0;
		foreach(lc, column_metadata->secondary_index_attnums) {
			int attnum = lfirst_int(lc);
			if (attnum == var->varattno) {
				shippable_condition->condtition_type =
					SCALARDB_SECONDARY_INDEX_EQ;
				shippable_condition->column = var;
				shippable_condition->name = list_nth(
					column_metadata->secondary_index_names,
					i);
				shippable_condition->expr =
					(Expr *)make_boolean_const(false);
				return true;
			}
			i++;
		}
		return false;
	}
	case T_OpExpr: {
		ListCell *lc;
		int i;
		Var *left;
		Node *right;
		OpExpr *op = (OpExpr *)expr;
		ScalarDbFdwOperator op_type = get_operator_type(op);

		if (op_type == SCALARDB_OP_OTHER)
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
			i = 0;
			foreach(lc, column_metadata->partition_key_attnums) {
				int attnum = lfirst_int(lc);
				if (attnum == left->varattno) {
					shippable_condition->condtition_type =
						SCALARDB_PARTITION_KEY_EQ;
					shippable_condition->column = left;
					shippable_condition->name = list_nth(
						column_metadata
							->partition_key_names,
						i);
					shippable_condition->expr =
						(Expr *)right;
					return true;
				}
				i++;
			}
		}
		if (op_type == SCALARDB_OP_LE || op_type == SCALARDB_OP_GT) {
			i = 0;
			foreach(lc, column_metadata->clustering_key_attnums) {
				int attnum = lfirst_int(lc);
				if (attnum == left->varattno) {
					shippable_condition->condtition_type =
						op_type == SCALARDB_OP_LE ?
							SCALARDB_CLUSTERING_KEY_LE :
							SCALARDB_CLUSTERING_KEY_GT;
					shippable_condition->column = left;
					shippable_condition->name = list_nth(
						column_metadata
							->clustering_key_names,
						i);
					shippable_condition->expr =
						(Expr *)right;
					return true;
				}
				i++;
			}
		}
		if (op_type == SCALARDB_OP_EQ) {
			i = 0;
			foreach(lc, column_metadata->secondary_index_attnums) {
				int attnum = lfirst_int(lc);
				if (attnum == left->varattno) {
					shippable_condition->condtition_type =
						SCALARDB_SECONDARY_INDEX_EQ;
					shippable_condition->column = left;
					shippable_condition->name = list_nth(
						column_metadata
							->secondary_index_names,
						i);
					shippable_condition->expr =
						(Expr *)right;
					return true;
				}
				i++;
			}
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
	ScalarDbFdwOperator ret = SCALARDB_OP_OTHER;

	if (list_length(op->args) != 2)
		return false;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", op->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);

	if (strcmp(NameStr(form->oprname), "=") == 0)
		ret = SCALARDB_OP_EQ;
	else if (strcmp(NameStr(form->oprname), "<=") == 0)
		ret = SCALARDB_OP_LE;
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
