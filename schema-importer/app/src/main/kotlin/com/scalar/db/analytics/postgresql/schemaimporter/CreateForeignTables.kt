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
import com.scalar.db.api.TableMetadata
import com.scalar.db.io.DataType
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class CreateForeignTables(
    private val ctx: DatabaseContext,
    private val namespaces: Set<String>,
    private val storage: ScalarDBStorage,
    private val admin: DistributedStorageAdmin,
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
                            "Table metadata not found: $ns.$tableName",
                        )
                val foreignTableName = "$ns._$tableName"
                val columnDefinitions = getForeignTableColumnDefinitions(metadata)
                val options = getForeignTableOptions(ns, tableName, storageForNamespace)

                logger.info {
                    "Creating foreign table: $foreignTableName for ${storageForNamespace.serverName}"
                }
                ctx.useStatement {
                    executeUpdateWithLogging(
                        it,
                        logger,
                        """
                            |CREATE FOREIGN TABLE IF NOT EXISTS $foreignTableName (
                            |$columnDefinitions
                            |) SERVER ${storageForNamespace.serverName} 
                            |OPTIONS (${options.joinToString(", ")});
                            """
                            .trimMargin(),
                    )
                }
            }
        }
    }

    private fun getForeignTableColumnDefinitions(
        metadata: TableMetadata,
        indent: String = "    ",
    ): String = getColumnInfoList(metadata).joinToString(",\n") { (col, typ) -> "$indent$col $typ" }

    private fun getColumnInfoList(
        metadata: TableMetadata,
    ): List<Pair<String, String>> =
        metadata.columnNames.map { col ->
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
        storage: ScalarDBStorage.SingleStorage,
    ): Set<String> =
        if (useScalarDBFdw(storage)) {
            getForeignTableOptionsForScalarDBFdw(namespace, tableName)
        } else {
            getForeignTableOptionsForNativeFdw(namespace, tableName, storage)
        }

    private fun getForeignTableOptionsForNativeFdw(
        namespace: String,
        tableName: String,
        storage: ScalarDBStorage.SingleStorage,
    ): Set<String> =
        when (storage) {
            is ScalarDBStorage.Jdbc -> setOf("schema_name '$namespace'", "table_name '$tableName'")
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
