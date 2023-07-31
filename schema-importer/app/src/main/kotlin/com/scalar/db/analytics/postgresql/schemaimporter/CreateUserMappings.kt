package com.scalar.db.analytics.postgresql.schemaimporter

import mu.KotlinLogging
import kotlin.jvm.optionals.getOrDefault

private val logger = KotlinLogging.logger {}

class CreateUserMappings(
    private val ctx: DatabaseContext,
    private val storage: ScalarDBStorage,
) {
    fun run() {
        when (storage) {
            is ScalarDBStorage.SingleStorage -> {
                logger.info { "Create user mapping: ${storage.serverName}" }
                if (useScalarDBFdw(storage)) {
                    createEmptyUserMapping(storage)
                } else {
                    createSingleUserMapping(storage)
                }
            }
            is ScalarDBStorage.MultiStorage -> createMultipleUserMappings(storage)
        }
    }

    private fun createSingleUserMapping(storage: ScalarDBStorage.SingleStorage) {
        val user = storage.config.username.getOrDefault("")!!
        val password = storage.config.password.getOrDefault("")!!
        ctx.useStatement {
            executeUpdateWithLogging(
                it,
                logger,
                """
                |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${storage.serverName}
                |OPTIONS (username '$user', password '$password');
                """
                    .trimMargin(),
            )
        }
    }
    private fun createEmptyUserMapping(storage: ScalarDBStorage.SingleStorage) {
        ctx.useStatement {
            executeUpdateWithLogging(
                it,
                logger,
                "CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${storage.serverName};",
            )
        }
    }

    private fun createMultipleUserMappings(multiStorage: ScalarDBStorage.MultiStorage) {
        for ((name, storage) in multiStorage.storages) {
            logger.info { "Create user mapping: ${storage.serverName}" }
            if (useScalarDBFdw(storage)) {
                createEmptyUserMapping(storage)
            } else {
                createSingleUserMapping(storage)
            }
        }
    }
}
