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
class CreateExtensionTest {
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
    fun `run should create jdbc_fdw and scalardb_fdw extension for jdbc storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Jdbc(config)
        CreateExtension(ctx, storage).run()

        verify {
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "jdbc_fdw";""")
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "scalardb_fdw";""")
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create cassandra2_fdw extension for cassandra storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cassandra(config)
        CreateExtension(ctx, storage).run()

        verify {
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "cassandra2_fdw";""")
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create scalardb_fdw extension for cosmos storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cosmos(config)
        CreateExtension(ctx, storage).run()

        verify {
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "scalardb_fdw";""")
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create scalardb_fdw extension for dynamodb storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.DynamoDB(config)
        CreateExtension(ctx, storage).run()

        verify {
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "scalardb_fdw";""")
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create all required fdw extensions for multi storage`() {
        val jdbcConfig = mockk<DatabaseConfig>()
        val cassandraConfig = mockk<DatabaseConfig>()
        val cosmosConfig = mockk<DatabaseConfig>()
        val dynamodbConfig = mockk<DatabaseConfig>()

        val storages =
            mapOf(
                "jdbc" to ScalarDBStorage.Jdbc(jdbcConfig),
                "cassandra" to ScalarDBStorage.Cassandra(cassandraConfig),
                "cosmos" to ScalarDBStorage.Cosmos(cosmosConfig),
                "dynamodb" to ScalarDBStorage.DynamoDB(dynamodbConfig),
            )

        val namespaceStorageMap =
            mapOf(
                "ns_for_jdbc" to "jdbc",
                "ns_for_cassandra" to "cassandra",
                "ns_for_cosmos" to "cosmos",
                "ns_for_dynamodb" to "dynamodb",
            )

        val storage = ScalarDBStorage.MultiStorage(storages, namespaceStorageMap)

        CreateExtension(ctx, storage).run()

        verify {
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "jdbc_fdw";""")
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "cassandra2_fdw";""")
            statement.executeUpdate("""CREATE EXTENSION IF NOT EXISTS "scalardb_fdw";""")
            statement.close()
        }
        confirmVerified(statement)
    }
}
