package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.transaction.consensuscommit.Attribute
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

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
                                "${indent}$c"
                            } else {
                                "${indent}CASE " +
                                    // Use the current value if the row is in COMMITTED state
                                    "WHEN ${Attribute.STATE} = 3 OR ${Attribute.STATE} IS NULL THEN $c " +
                                    // Use the value in the before image if the row is under
                                    // transaction processing
                                    "ELSE ${Attribute.BEFORE_PREFIX}$c END AS $c"
                            }
                        }

                val whereClause =
                    if (transactionEnabled) {
                        // In COMMITTED state
                        "WHERE ${Attribute.STATE} = 3 OR " +
                            // Committed before being integrated with ScalarDB.
                            "${Attribute.STATE} IS NULL OR " +
                            // Committed in the past
                            "${Attribute.BEFORE_STATE} = 3"
                    } else {
                        ""
                    }

                val viewName = "$ns.$tableName"
                val rawTableName = "$ns._$tableName"

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
