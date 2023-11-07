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

import mu.KLogger
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

class DatabaseContext(private val connection: Connection) {
    fun <T> useStatement(f: (Statement) -> T): T = this.connection.createStatement().use { f(it) }
}

fun <T> useDatabaseContext(
    url: String,
    user: String,
    password: String,
    f: (ctx: DatabaseContext) -> T,
): T =
    DriverManager.getConnection(url, user, password).use { conn ->
        val ctx = DatabaseContext(conn)
        conn.autoCommit = false
        val ret = f(ctx)
        conn.commit()
        ret
    }

fun executeUpdateWithLogging(stmt: Statement, logger: KLogger, sql: String) {
    logger.debug { sql }
    stmt.executeUpdate(sql)
}
