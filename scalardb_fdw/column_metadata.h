#ifndef SCALARDB_FDW_UTIL_H
#define SCALARDB_FDW_UTIL_H

#include "c.h"
#include "postgres.h"

#include "optimizer/planmain.h"
#include "nodes/pg_list.h"

/*
 * Column metadata of ScalarDB table.
 */
typedef struct {
	/* column names each key type
	 * The types are List of String */
	List *partition_key_names;
	List *clustering_key_names;
	List *secondary_index_names;

	/* attnum in Form_pg_attribute for each key type
	 * The types are T_IntList*/
	List *partition_key_attnums;
	List *clustering_key_attnums;
	List *secondary_index_attnums;

	/* List of ScalarDbFdwClusteringKeyOrder */
	List *clustering_key_orders;
} ScalarDbFdwColumnMetadata;

extern void get_column_metadata(PlannerInfo *root, RelOptInfo *baserel,
				char *namespace, char *table_name,
				ScalarDbFdwColumnMetadata *column_metadata);

#endif
