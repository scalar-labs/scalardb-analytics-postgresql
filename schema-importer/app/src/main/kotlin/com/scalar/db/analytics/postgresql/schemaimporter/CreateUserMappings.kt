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
import kotlin.jvm.optionals.getOrDefault

private val logger = KotlinLogging.logger {}

class CreateUserMappings(
    private val ctx: DatabaseContext,
    private val storage: ScalarDBStorage,
) {
    fun run() {
        when (storage) {
            is ScalarDBStorage.SingleStorage -> {
                logger.info { "Create user mapping: ${escapeIdentifier(storage.serverName)}" }
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
                |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${escapeIdentifier(storage.serverName)}
                |OPTIONS (username '${escapeLiteral(user)}', password '${escapeLiteral(password)}');
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
                "CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER ${escapeIdentifier(storage.serverName)};",
            )
        }
    }

    private fun createMultipleUserMappings(multiStorage: ScalarDBStorage.MultiStorage) {
        for ((name, storage) in multiStorage.storages) {
            logger.info { "Create user mapping: ${escapeIdentifier(storage.serverName)}" }
            if (useScalarDBFdw(storage)) {
                createEmptyUserMapping(storage)
            } else {
                createSingleUserMapping(storage)
            }
        }
    }
}
