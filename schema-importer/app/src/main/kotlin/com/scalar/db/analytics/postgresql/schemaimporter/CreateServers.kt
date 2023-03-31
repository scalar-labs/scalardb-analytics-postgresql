package com.scalar.db.analytics.postgresql.schemaimporter

import java.nio.file.Path

class CreateServers(
    private val ctx: DatabaseContext,
    private val storage: ScalarDBStorage,
    private val configPath: Path
) {
    fun run() {
        when (storage) {
            is ScalarDBStorage.SingleStorage ->
                if (storage.useNativeFdw) {
                    createServerWithNativeFdw(storage)
                } else {
                    createServerWithScalarDBFdw(storage, configPath)
                }
            is ScalarDBStorage.MultiStorage -> createServerForMultiStorage(storage)
        }
    }

    private fun createServerForMultiStorage(multiStorage: ScalarDBStorage.MultiStorage) {
        for ((_, storage) in multiStorage.storages) {
            if (storage.useNativeFdw) {
                createServerWithNativeFdw(storage)
            } else {
                createServerWithScalarDBFdw(storage, configPath)
            }
        }
    }

    private fun createServerForCassandra(storage: ScalarDBStorage.Cassandra) =
        ctx.useStatement {
            it.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS ${storage.serverName}
                |FOREIGN DATA WRAPPER cassandra2_fdw
                |OPTIONS (host '${storage.host}', port '${storage.port}');
                """
                    .trimMargin()
            )
        }

    private fun createServerForJDBC(storage: ScalarDBStorage.JDBC) {
        val url = storage.url

        val driverName =
            when {
                url.startsWith("jdbc:postgresql:") -> "org.postgresql.Driver"
                url.startsWith("jdbc:mysql:") -> "com.mysql.jdbc.Driver"
                url.startsWith("jdbc:oracle:") -> "oracle.jdbc.OracleDriver"
                url.startsWith("jdbc:sqlserver:") -> "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                else -> throw IllegalArgumentException("Unsupported JDBC URL: $url")
            }

        val jarFile = getRunningJarFile()

        ctx.useStatement {
            it.executeUpdate(
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
                    .trimMargin()
            )
        }
    }

    private fun createServerWithNativeFdw(storage: ScalarDBStorage.SingleStorage) =
        when (storage) {
            is ScalarDBStorage.JDBC -> createServerForJDBC(storage)
            is ScalarDBStorage.Cassandra -> createServerForCassandra(storage)
            else ->
                throw IllegalArgumentException("Native FDW of ${storage.name} is not supported yet")
        }

    private fun createServerWithScalarDBFdw(
        storage: ScalarDBStorage.SingleStorage,
        configPath: Path
    ) =
        ctx.useStatement {
            it.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS ${storage.serverName}
                |FOREIGN DATA WRAPPER scalardb_fdw
                |OPTIONS (config_file_path '${configPath.toAbsolutePath()}');
                """
                    .trimMargin()
            )
        }
}
