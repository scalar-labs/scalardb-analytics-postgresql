#include "c.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "postgres.h"

#include "access/stratnum.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "nodes/pathnodes.h"

#include "pathkeys.h"
#include "condition.h"
#include "scalardb.h"
#include "scalardb_fdw.h"
#include "column_metadata.h"
#include "cost.h"

static void add_path_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
				   List *pathkeys, List *fdw_private_for_path);

static bool is_clustering_key_sort(List *query_pathkeys, RelOptInfo *rel,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **sort_column_names,
				   List **sort_orders);

static EquivalenceMember *find_em_for_rel(EquivalenceClass *ec,
					  RelOptInfo *rel);

static bool is_same_order(PathKey *pathkey,
			  ScalarDbFdwClusteringKeyOrder order);

static ScalarDbFdwClusteringKeyOrder
get_sort_order(ScalarDbFdwClusteringKeyOrder order, bool same_order);

extern void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
				ScalarDbFdwColumnMetadata *column_metadata)
{
	ereport(DEBUG3, errmsg("entering function %s", __func__));

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys) {
		List *sort_column_names = NIL;
		List *sort_orders = NIL;
		if (is_clustering_key_sort(root->query_pathkeys, rel,
					   column_metadata, &sort_column_names,
					   &sort_orders)) {
			List *fdw_private_for_path =
				list_make2(sort_column_names, sort_orders);
			add_path_with_pathkeys(root, rel, root->query_pathkeys,
					       fdw_private_for_path);
		}
	}
	/* TODO: consider userful orderings for mergejoin */
}

static void add_path_with_pathkeys(PlannerInfo *root, RelOptInfo *rel,
				   List *pathkeys, List *fdw_private_for_path)
{
	double rows;
	Cost startup_cost;
	Cost total_cost;

	ScalarDbFdwPlanState *fdw_private =
		(ScalarDbFdwPlanState *)rel->fdw_private;

	estimate_costs(root, rel, fdw_private->remote_conds, &rows,
		       &startup_cost, &total_cost);

	add_path(rel, (Path *)create_foreignscan_path(
			      root, rel, NULL, rows, startup_cost, total_cost,
			      pathkeys, rel->lateral_relids, NULL,
			      fdw_private_for_path));
}

/*
 * Returns true if query_pathkeys (i.e., final output ordering) can be represented as a sort 
 * based on clustering keys of the given foreign ScalarDb relation.
 *
 * If true, the column names used to sort the given relation are pushed in sort_column_names,
 * and the sort orders for each column are pushed into sort_orders.
 */
static bool is_clustering_key_sort(List *query_pathkeys, RelOptInfo *rel,
				   ScalarDbFdwColumnMetadata *column_metadata,
				   List **sort_column_names, List **sort_orders)
{
	ListCell *lc_pk;
	ListCell *lc_attnum;
	ListCell *lc_name;
	ListCell *lc_order;

	bool same_order;

	/* The entire query_pathkeys must be represented by sorting using some or all of the clustering keys */
	if (list_length(query_pathkeys) >
	    list_length(column_metadata->clustering_key_attnums))
		return false;

	forfour(lc_pk, query_pathkeys, lc_attnum,
		column_metadata->clustering_key_attnums, lc_name,
		column_metadata->clustering_key_names, lc_order,
		column_metadata->clustering_key_orders)
	{
		PathKey *pathkey = (PathKey *)lfirst(lc_pk);
		int attnum = lfirst_int(lc_attnum);
		String *name = lfirst_node(String, lc_name);
		ScalarDbFdwClusteringKeyOrder order =
			(ScalarDbFdwClusteringKeyOrder)lfirst_int(lc_order);
		int index = foreach_current_index(lc_pk);

		EquivalenceMember *em;
		Var *var;

		if (pathkey->pk_eclass->ec_has_volatile)
			return false;

		em = find_em_for_rel(pathkey->pk_eclass, rel);
		if (em == NULL)
			return false;
		var = (Var *)em->em_expr;

		if (var->varattno != attnum)
			return false;

		/* pathkeys and clustering key orders must all be in the same order or all in reverse order */
		if (index == 0) {
			same_order = is_same_order(pathkey, order);
		} else {
			if (same_order != is_same_order(pathkey, order))
				return false;
		}
		*sort_column_names = lappend(*sort_column_names, name);
		*sort_orders = lappend_int(*sort_orders,
					   get_sort_order(order, same_order));
	}

	return true;
}

/*
 * Given an EquivalenceClass and a foreign relation, find an EC member
 * that can be used to sort the relation remotely according to a pathkey
 * using this EC.
 *
 * If there is more than one suitable candidate, return an arbitrary
 * one of them.  If there is none, return NULL.
 *
 * This checks that the EC member expression uses only Vars from the given rel.
 */
EquivalenceMember *find_em_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell *lc;

	foreach(lc, ec->ec_members) {
		EquivalenceMember *em = (EquivalenceMember *)lfirst(lc);

		/*
		 * Note we require !bms_is_empty, else we'd accept constant
		 * expressions which are not suitable for the purpose.
		 */
		if (bms_is_subset(em->em_relids, rel->relids) &&
		    !bms_is_empty(em->em_relids) &&
		    is_foreign_table_var(em->em_expr, rel))
			return em;
	}

	return NULL;
}

/*
 * Returns true if the given pathkey and clustering key order are the same.
 */
static bool is_same_order(PathKey *pathkey, ScalarDbFdwClusteringKeyOrder order)
{
	return (pathkey->pk_strategy == BTLessStrategyNumber &&
		order == SCALARDB_CLUSTERING_KEY_ORDER_ASC) ||
	       (pathkey->pk_strategy == BTGreaterStrategyNumber &&
		order == SCALARDB_CLUSTERING_KEY_ORDER_DESC);
}

static ScalarDbFdwClusteringKeyOrder
get_sort_order(ScalarDbFdwClusteringKeyOrder order, bool same_order)
{
	switch (order) {
	case SCALARDB_CLUSTERING_KEY_ORDER_ASC:
		return same_order ? SCALARDB_CLUSTERING_KEY_ORDER_ASC :
				    SCALARDB_CLUSTERING_KEY_ORDER_DESC;
	case SCALARDB_CLUSTERING_KEY_ORDER_DESC:
		return same_order ? SCALARDB_CLUSTERING_KEY_ORDER_DESC :
				    SCALARDB_CLUSTERING_KEY_ORDER_ASC;
	}
}
