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

private val logger = KotlinLogging.logger {}

class CreateSchema(private val ctx: DatabaseContext, private val namespaces: Set<String>) {
    fun run() {
        for (ns in namespaces) {
            ctx.useStatement() {
                logger.info { "Creating schema: $ns" }
                executeUpdateWithLogging(it, logger, "CREATE SCHEMA IF NOT EXISTS $ns;")
            }
        }
    }
}
