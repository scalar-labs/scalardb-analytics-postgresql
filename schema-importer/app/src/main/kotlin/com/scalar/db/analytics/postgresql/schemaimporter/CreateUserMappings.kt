package com.scalar.db.analytics.postgresql.schemaimporter

import kotlin.jvm.optionals.getOrDefault

class CreateUserMappings(
    private val ctx: DatabaseContext,
    private val storage: ScalarDBStorage,
) {
    fun run() {
        when (storage) {
            is ScalarDBStorage.SingleStorage ->
                if (useScalarDBFdw(storage)) {
                    createEmptyUserMapping(storage)
                } else {
                    createSingleUserMapping(storage)
                }
            is ScalarDBStorage.MultiStorage -> createMultipleUserMappings(storage)
        }
    }

    private fun createSingleUserMapping(storage: ScalarDBStorage.SingleStorage) {
        val user = storage.config.username.getOrDefault("")!!
        val password = storage.config.password.getOrDefault("")!!
        ctx.useStatement {
            it.executeUpdate(
                """
                |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${storage.serverName}
                |OPTIONS (username '$user', password '$password');
                """
                    .trimMargin()
            )
        }
    }
    private fun createEmptyUserMapping(storage: ScalarDBStorage.SingleStorage) {
        ctx.useStatement {
            it.executeUpdate(
                "CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${storage.serverName};"
            )
        }
    }

    private fun createMultipleUserMappings(multiStorage: ScalarDBStorage.MultiStorage) {
        for ((_, storage) in multiStorage.storages) {
            if (useScalarDBFdw(storage)) {
                createEmptyUserMapping(storage)
            } else {
                createSingleUserMapping(storage)
            }
        }
    }
}
