package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.api.TableMetadata
import com.scalar.db.io.DataType

class CreateForeignTables(
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

            for (tableName in admin.getNamespaceTableNames(ns)) {
                val metadata =
                    admin.getTableMetadata(ns, tableName)
                        ?: throw IllegalArgumentException(
                            "Table metadata not found: $ns.$tableName"
                        )
                val foreignTableName =
                    if (storageForNamespace.useNativeFdw) "$ns._$tableName" else "$ns.$tableName"
                val columnDefinitions =
                    getForeignTableColumnDefinitions(metadata, storageForNamespace)
                val options = getForeignTableOptions(ns, tableName, storageForNamespace)
                ctx.useStatement {
                    it.executeUpdate(
                        """
                            |CREATE FOREIGN TABLE IF NOT EXISTS $foreignTableName (
                            |$columnDefinitions
                            |) SERVER ${storageForNamespace.serverName} 
                            |OPTIONS (${options.joinToString(", ")});
                            """
                            .trimMargin()
                    )
                }
            }
        }
    }

    private fun getForeignTableColumnDefinitions(
        metadata: TableMetadata,
        storage: ScalarDBStorage.SingleStorage,
        indent: String = "    "
    ): String =
        getColumnInfoList(metadata, storage).joinToString(",\n") { (col, typ) ->
            "$indent$col $typ"
        }

    private fun getColumnInfoList(
        metadata: TableMetadata,
        storage: ScalarDBStorage.SingleStorage
    ): List<Pair<String, String>> =
        metadata.columnNames
            .filter { storage.useNativeFdw || isUserColumn(it) }
            .map { col ->
                val typ = metadata.getColumnDataType(col)
                col to getPgType(typ)
            }

    private fun getPgType(typ: DataType): String =
        when (typ) {
            DataType.BOOLEAN -> "boolean"
            DataType.INT -> "int"
            DataType.BIGINT -> "bigint"
            DataType.FLOAT -> "float"
            DataType.DOUBLE -> "double precision"
            DataType.TEXT -> "text"
            DataType.BLOB -> "bytea"
        }

    private fun getForeignTableOptions(
        namespace: String,
        tableName: String,
        storage: ScalarDBStorage.SingleStorage
    ): Set<String> =
        if (storage.useNativeFdw) {
            getForeignTableOptionsForNativeFdw(namespace, tableName, storage)
        } else {
            getForeignTableOptionsForScalarDBFdw(namespace, tableName)
        }

    private fun getForeignTableOptionsForNativeFdw(
        namespace: String,
        tableName: String,
        storage: ScalarDBStorage.SingleStorage
    ): Set<String> =
        when (storage) {
            is ScalarDBStorage.JDBC -> setOf("schema_name '$namespace'", "table_name '$tableName'")
            is ScalarDBStorage.Cassandra ->
                setOf("schema_name '$namespace'", "table_name '$tableName'")
            else ->
                throw IllegalArgumentException("Native FDW of ${storage.name} is not supported yet")
        }

    private fun getForeignTableOptionsForScalarDBFdw(
        namespace: String,
        tableName: String,
    ): Set<String> = setOf("namespace '$namespace'", "table_name '$tableName'")
}
