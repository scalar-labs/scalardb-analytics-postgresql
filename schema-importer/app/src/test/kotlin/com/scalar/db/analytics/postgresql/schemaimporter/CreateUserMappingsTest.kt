package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.config.DatabaseConfig
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.extension.ExtendWith
import java.sql.Connection
import java.sql.Statement
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test

@ExtendWith(MockKExtension::class)
class CreateUserMappingsTest {
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
    fun `run should create user mappings with configures user and password for jdbc`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:postgresql://host:port/database")
        every { config.username } returns Optional.of("user")
        every { config.password } returns Optional.of("password")
        val storage = ScalarDBStorage.Jdbc(config)
        CreateUserMappings(ctx, storage).run()

        verify {
            statement.executeUpdate(
                """
               |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER jdbc
               |OPTIONS (username 'user', password 'password');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create user mappings with configures user and password for cassandra`() {
        val config = mockk<DatabaseConfig>()
        every { config.username } returns Optional.of("user")
        every { config.password } returns Optional.of("password")
        val storage = ScalarDBStorage.Cassandra(config)
        CreateUserMappings(ctx, storage).run()

        verify {
            statement.executeUpdate(
                """
               |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER cassandra
               |OPTIONS (username 'user', password 'password');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create an empty user mapping for scalardb_fdw`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cosmos(config)
        CreateUserMappings(ctx, storage).run()

        verify {
            statement.executeUpdate(
                """
               |CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER cosmos;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }
}
