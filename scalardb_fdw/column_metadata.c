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
#include "c.h"
#include "postgres.h"

#include "access/table.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "utils/rel.h"

#include "scalardb.h"
#include "column_metadata.h"

extern void get_column_metadata(PlannerInfo *root, RelOptInfo *baserel,
				char *namespace, char *table_name,
				ScalarDbFdwColumnMetadata *column_metadata)
{
	RangeTblEntry *rte;
	Relation rel;
	TupleDesc tupdesc;
	ListCell *lc;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	scalardb_get_paritition_key_names(
		namespace, table_name, &column_metadata->partition_key_names);
	scalardb_get_clustering_key_names_and_orders(
		namespace, table_name, &column_metadata->clustering_key_names,
		&column_metadata->clustering_key_orders);
	scalardb_get_secondary_index_names(
		namespace, table_name, &column_metadata->secondary_index_names);

	rte = planner_rt_fetch(baserel->relid, root);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);
	tupdesc = RelationGetDescr(rel);

	for (int attnum = 1; attnum <= tupdesc->natts; attnum++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		foreach(lc, column_metadata->partition_key_names) {
			char *name = strVal(lfirst(lc));
			if (strcmp(NameStr(attr->attname), name) == 0) {
				column_metadata
					->partition_key_attnums = lappend_int(
					column_metadata->partition_key_attnums,
					attr->attnum);
				goto NEXT_COLUMN;
			}
		}
		foreach(lc, column_metadata->clustering_key_names) {
			char *name = strVal(lfirst(lc));
			if (strcmp(NameStr(attr->attname), name) == 0) {
				column_metadata
					->clustering_key_attnums = lappend_int(
					column_metadata->clustering_key_attnums,
					attr->attnum);
				goto NEXT_COLUMN;
			}
		}
		foreach(lc, column_metadata->secondary_index_names) {
			char *name = strVal(lfirst(lc));
			if (strcmp(NameStr(attr->attname), name) == 0) {
				column_metadata
					->secondary_index_attnums = lappend_int(
					column_metadata->secondary_index_attnums,
					attr->attnum);
				goto NEXT_COLUMN;
			}
		}
NEXT_COLUMN:;
		continue;
	}
	table_close(rel, NoLock);
}
