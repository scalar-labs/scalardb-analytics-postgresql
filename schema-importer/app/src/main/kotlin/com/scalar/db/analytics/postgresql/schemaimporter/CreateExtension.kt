package com.scalar.db.analytics.postgresql.schemaimporter

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

class CreateExtension(private val ctx: DatabaseContext, private val storage: ScalarDBStorage) {
    fun run() {
        ctx.useStatement() { stmt ->
            for (fdw in storageToFdw(storage)) {
                logger.info { "Creating extension: $fdw" }
                executeUpdateWithLogging(stmt, logger, "CREATE EXTENSION IF NOT EXISTS \"$fdw\";")
            }
        }
    }
}
