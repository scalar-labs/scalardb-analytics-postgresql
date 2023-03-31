package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin

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
            if (storageForNamespace.useNativeFdw.not()) {
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
                val transactionEnabled = columns.contains(Constant.TX_STATE_COL)

                val indent = "    "
                val columnsList =
                    columns
                        .filter { isUserColumn(it) }
                        .joinToString(",\n") { c ->
                            if (transactionEnabled.not() || pks.contains(c) || cks.contains(c)) {
                                "${indent}$c"
                            } else {
                                "${indent}CASE WHEN ${Constant.TX_STATE_COL} = 3 THEN $c ELSE before_$c END AS $c"
                            }
                        }

                val whereClause =
                    if (transactionEnabled) {
                        "WHERE ${Constant.TX_STATE_COL} = 3 OR before_tx_state IS NOT NULL"
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
