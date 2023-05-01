package com.scalar.db.analytics.postgresql.schemaimporter

import mu.KotlinLogging

private val logger = KotlinLogging.logger{}
class CreateSchema(private val ctx: DatabaseContext, private val namespaces: Set<String>) {
    fun run() {
        for (ns in namespaces) {
            ctx.useStatement() {
                logger.info {"Creating schema: $ns"}
                executeUpdateWithLogging(it, logger, "CREATE SCHEMA IF NOT EXISTS $ns;")
            }
        }
    }
}
