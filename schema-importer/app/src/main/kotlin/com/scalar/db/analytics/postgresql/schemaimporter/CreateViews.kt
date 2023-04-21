package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.transaction.consensuscommit.Attribute
import com.scalar.db.transaction.consensuscommit.ConsensusCommitUtils

class CreateViews(
    private val ctx: DatabaseContext,
    private val namespaces: Set<String>,
    private val storage: ScalarDBStorage,
    private val admin: DistributedStorageAdmin
) {
    fun run() {
        for (ns in namespaces) {
            val storageForNamespace: ScalarDBStorage.SingleStorage =
                when (storage) {
                    is ScalarDBStorage.MultiStorage -> storage.getStorageForNamespace(ns)
                    is ScalarDBStorage.SingleStorage -> storage
                }

            /** View is not needed for storages that use scalardb_fdw to access */
            if (useScalarDBFdw(storageForNamespace)) {
                continue
            }

            for (tableName in admin.getNamespaceTableNames(ns)) {
                val metadata =
                    admin.getTableMetadata(ns, tableName)
                        ?: throw IllegalArgumentException(
                            "Table metadata not found: $ns.$tableName"
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
                                "${indent}CASE WHEN ${Attribute.STATE} = 3 THEN $c ELSE ${Attribute.BEFORE_PREFIX}$c END AS $c"
                            }
                        }

                val whereClause =
                    if (transactionEnabled) {
                        "WHERE ${Attribute.STATE} = 3 OR ${Attribute.BEFORE_STATE} IS NOT NULL"
                    } else ""

                val viewName = "$ns.$tableName"
                val rawTableName = "$ns._$tableName"

                ctx.useStatement {
                    it.executeUpdate(
                        """
                            |CREATE OR REPLACE VIEW $viewName AS
                            |SELECT
                            |$columnsList
                            |FROM $rawTableName
                            |$whereClause;
                            """
                            .trimMargin()
                    )
                }
            }
        }
    }
}
