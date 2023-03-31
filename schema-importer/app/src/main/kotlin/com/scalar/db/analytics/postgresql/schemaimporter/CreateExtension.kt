package com.scalar.db.analytics.postgresql.schemaimporter

class CreateExtension(private val ctx: DatabaseContext, private val storage: ScalarDBStorage) {
    fun run() {
        ctx.useStatement() { stmt ->
            for (fdw in storageToFdw(storage)) {
                stmt.executeUpdate("CREATE EXTENSION IF NOT EXISTS \"$fdw\";")
            }
        }
    }

    private fun storageToFdw(storage: ScalarDBStorage): Set<String> {
        return when (storage) {
            is ScalarDBStorage.SingleStorage -> {
                if (storage.useNativeFdw) {
                    when (storage) {
                        is ScalarDBStorage.Cassandra -> setOf("cassandra2_fdw")
                        is ScalarDBStorage.JDBC -> setOf("jdbc_fdw")
                        else ->
                            throw IllegalArgumentException(
                                "Native FDW extension for $storage is not supported"
                            )
                    }
                } else {
                    setOf("scalardb_fdw")
                }
            }
            is ScalarDBStorage.MultiStorage -> {
                storage.storages.values.map { storageToFdw(it) }.flatten().toSet()
            }
        }
    }
}
