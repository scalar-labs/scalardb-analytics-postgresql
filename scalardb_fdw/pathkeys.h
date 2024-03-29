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
