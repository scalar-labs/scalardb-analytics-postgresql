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
package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.transaction.consensuscommit.Attribute
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}
private val escapedStateColumn: String = escapeIdentifier(Attribute.STATE)
private val escapedBeforeStateColumn: String = escapeIdentifier(Attribute.BEFORE_STATE)

class CreateViews(
    private val ctx: DatabaseContext,
    private val namespaces: Set<String>,
    private val admin: DistributedStorageAdmin,
) {

    fun run() {
        for (ns in namespaces) {
            for (tableName in admin.getNamespaceTableNames(ns)) {
                val metadata =
                    admin.getTableMetadata(ns, tableName)
                        ?: throw IllegalArgumentException(
                            "Table metadata not found: $ns.$tableName",
                        )

                val pks = metadata.partitionKeyNames
                val cks = metadata.clusteringKeyNames
                val columns = metadata.columnNames
                val transactionEnabled = ConsensusCommitUtils.isTransactionTableMetadata(metadata)

                val indent = "    "
                val columnsList =
                    columns
                        .filter { !ConsensusCommitUtils.isTransactionMetaColumn(it, metadata) }
                        .joinToString(",\n") { c ->
                            if (transactionEnabled.not() || pks.contains(c) || cks.contains(c)) {
                                "${indent}${escapeIdentifier(c)}"
                            } else {
                                "${indent}CASE " +
                                    // Use the current value if the row is in COMMITTED state
                                    "WHEN $escapedStateColumn = 3 OR $escapedStateColumn IS NULL THEN ${escapeIdentifier(c)} " +
                                    // Use the value in the before image if the row is under
                                    // transaction processing
                                    "ELSE ${escapeIdentifier(Attribute.BEFORE_PREFIX + c)} END AS ${escapeIdentifier(c)}"
                            }
                        }

                val whereClause =
                    if (transactionEnabled) {
                        // In COMMITTED state
                        "WHERE $escapedStateColumn = 3 OR " +
                            // Committed before being integrated with ScalarDB.
                            "$escapedStateColumn IS NULL OR " +
                            // Committed in the past
                            "$escapedBeforeStateColumn = 3"
                    } else {
                        ""
                    }

                val viewName = "${escapeIdentifier(ns)}.${escapeIdentifier(tableName)}"
                val rawTableName = "${escapeIdentifier(ns)}.${escapeIdentifier("_$tableName")}"

                logger.info { "Creating view: $viewName for $rawTableName" }
                ctx.useStatement {
                    executeUpdateWithLogging(
                        it,
                        logger,
                        """
                            |CREATE OR REPLACE VIEW $viewName AS
                            |SELECT
                            |$columnsList
                            |FROM $rawTableName
                            |$whereClause;
                            """
                            .trimMargin(),
                    )
                }
            }
        }
    }
}
