package com.scalar.db.analytics.postgresql.schemaimporter

import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.extension.ExtendWith
import java.sql.Connection
import java.sql.Statement
import kotlin.test.BeforeTest
import kotlin.test.Test

@ExtendWith(MockKExtension::class)
class CreateSchemaTest {
    @MockK(relaxUnitFun = true)
    lateinit var connection: Connection

    @MockK(relaxUnitFun = true)
    lateinit var statement: Statement

    private lateinit var ctx: DatabaseContext

    @BeforeTest
    fun setup() {
        every { connection.createStatement() } returns statement
        every { statement.executeUpdate(any()) } returns 0
        ctx = DatabaseContext(connection)
    }

    @Test
    fun `run should create schema`() {
        val namespaces = setOf("ns1", "ns2")
        CreateSchema(ctx, namespaces).run()

        verify {
            statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS ns1;")
            statement.executeUpdate("CREATE SCHEMA IF NOT EXISTS ns2;")
            statement.close()
        }
        confirmVerified(statement)
    }
}
