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
#ifndef SCALARDB_FDW_H
#define SCALARDB_FDW_H

#include "c.h"
#include "postgres.h"

#include "condition.h"
#include "option.h"

/*
 * The plan state is set up in scalardbGetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in scalardbGetForeignPaths.
 */
typedef struct {
	ScalarDbFdwOptions options;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	/* Conditions on the index keys or secondary indexes that are pushed to ScalarDB side */
	List *remote_conds;
	/* Conditions that are evaluated locally */
	List *local_conds;
	/* the clustering keys that are pushed to ScalarDB side */
	ScalarDbFdwClusteringKeyBoundary boundary;
	/* Type of Scan executed on the ScalarDB side. This must be consistent with the condtitions in remote_conds */
	ScalarDbFdwScanType scan_type;

	/* set of the column metadata of the table*/
	ScalarDbFdwColumnMetadata column_metadata;
} ScalarDbFdwPlanState;

#endif
