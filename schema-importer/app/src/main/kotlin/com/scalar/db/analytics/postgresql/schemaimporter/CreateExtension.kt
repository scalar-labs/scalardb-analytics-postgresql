package com.scalar.db.analytics.postgresql.schemaimporter

class CreateExtension(private val ctx: DatabaseContext, private val storage: ScalarDBStorage) {
    fun run() {
        ctx.useStatement() { stmt ->
            for (fdw in storageToFdw(storage)) {
                stmt.executeUpdate("CREATE EXTENSION IF NOT EXISTS \"$fdw\";")
            }
        }
    }
}
