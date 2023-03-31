package com.scalar.db.analytics.postgresql.schemaimporter

class CreateSchema(private val ctx: DatabaseContext, private val namespaces: Set<String>) {
    fun run() {
        for (ns in namespaces) {
            ctx.useStatement() { it.executeUpdate("CREATE SCHEMA IF NOT EXISTS $ns;") }
        }
    }
}
