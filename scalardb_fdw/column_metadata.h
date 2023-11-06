/*
 * Copyright 2023 Scalar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
