package com.scalar.db.analytics.postgresql.schemaimporter

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
    f: (ctx: DatabaseContext) -> T
): T =
    DriverManager.getConnection(url, user, password).use { conn ->
        val ctx = DatabaseContext(conn)
        conn.autoCommit = false
        val ret = f(ctx)
        conn.commit()
        ret
    }
