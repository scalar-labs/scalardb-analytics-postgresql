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

import mu.KotlinLogging
import java.nio.file.Path

private val logger = KotlinLogging.logger {}

class CreateServers(
    private val ctx: DatabaseContext,
    private val storage: ScalarDBStorage,
    configPath: Path,
    configPathOnPostgresHost: Path? = null,
) {
    private val configPathForScalarDBFdw =
        configPathOnPostgresHost
            ?: // Only configPath is converted to an absolute path because configPathOnPostgres is a
            // path on a remote host
            configPath.toAbsolutePath().normalize()

    fun run() {
        when (storage) {
            is ScalarDBStorage.SingleStorage -> {
                logger.info { "Creating server: ${storage.name} as ${storage.serverName}" }
                if (useScalarDBFdw(storage)) {
                    createServerWithScalarDBFdw(storage)
                } else {
                    createServerWithNativeFdw(storage)
                }
            }
            is ScalarDBStorage.MultiStorage -> createServerForMultiStorage(storage)
        }
    }

    private fun createServerForMultiStorage(multiStorage: ScalarDBStorage.MultiStorage) {
        for ((name, storage) in multiStorage.storages) {
            logger.info { "Creating server: $name as ${storage.serverName}" }
            if (useScalarDBFdw(storage)) {
                createServerWithScalarDBFdw(storage)
            } else {
                createServerWithNativeFdw(storage)
            }
        }
    }

    private fun createServerForCassandra(storage: ScalarDBStorage.Cassandra): Unit =
        ctx.useStatement {
            executeUpdateWithLogging(
                it,
                logger,
                """
                |CREATE SERVER IF NOT EXISTS ${storage.serverName}
                |FOREIGN DATA WRAPPER cassandra2_fdw
                |OPTIONS (host '${storage.host}', port '${storage.port}');
                """
                    .trimMargin(),
            )
        }

    private fun createServerForJdbc(storage: ScalarDBStorage.Jdbc) {
        val url = storage.url

        val driverName =
            when {
                url.startsWith("jdbc:postgresql:") -> "org.postgresql.Driver"
                url.startsWith("jdbc:mysql:") -> "com.mysql.jdbc.Driver"
                url.startsWith("jdbc:oracle:") -> "oracle.jdbc.OracleDriver"
                url.startsWith("jdbc:sqlserver:") -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                else -> throw IllegalArgumentException("Unsupported JDBC URL: $url")
            }

        ctx.useStatement {
            val jarFile = findScalarDBFdwJarFile(it)
            executeUpdateWithLogging(
                it,
                logger,
                """
                |CREATE SERVER IF NOT EXISTS ${storage.serverName}
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername '$driverName',
                |  jarfile '$jarFile',
                |  url '$url',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin(),
            )
        }
    }

    private fun createServerWithNativeFdw(storage: ScalarDBStorage.SingleStorage): Unit =
        when (storage) {
            is ScalarDBStorage.Jdbc -> createServerForJdbc(storage)
            is ScalarDBStorage.Cassandra -> createServerForCassandra(storage)
            else ->
                throw IllegalArgumentException("Native FDW of ${storage.name} is not supported yet")
        }

    private fun createServerWithScalarDBFdw(storage: ScalarDBStorage.SingleStorage) =
        ctx.useStatement {
            executeUpdateWithLogging(
                it,
                logger,
                """
                |CREATE SERVER IF NOT EXISTS ${storage.serverName}
                |FOREIGN DATA WRAPPER scalardb_fdw
                |OPTIONS (config_file_path '${configPathForScalarDBFdw.toAbsolutePath()}');
                """
                    .trimMargin(),
            )
        }
}
