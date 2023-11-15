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

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.api.Scan
import com.scalar.db.api.TableMetadata
import com.scalar.db.config.DatabaseConfig
import com.scalar.db.io.DataType
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
class CreateForeignTablesTest {
    @MockK(relaxUnitFun = true)
    lateinit var connection: Connection

    @MockK(relaxUnitFun = true)
    lateinit var statement: Statement

    private lateinit var ctx: DatabaseContext

    @MockK(relaxUnitFun = true)
    lateinit var admin: DistributedStorageAdmin

    @MockK(relaxUnitFun = true)
    lateinit var metadata: TableMetadata

    @BeforeTest
    fun setup() {
        every { connection.createStatement() } returns statement
        every { statement.executeUpdate(any()) } returns 0
        ctx = DatabaseContext(connection)

        every { admin.getNamespaceTableNames("ns_for_jdbc") } returns setOf("jdbc_table")
        every { admin.getNamespaceTableNames("ns_for_cassandra") } returns setOf("cassandra_table")
        every { admin.getNamespaceTableNames("ns_for_cosmos") } returns setOf("cosmos_table")
        every { admin.getNamespaceTableNames("ns_for_dynamodb") } returns setOf("dynamodb_table")
        every { admin.getTableMetadata(any(), any()) } returns
            TableMetadata.newBuilder()
                .addColumn("pk", DataType.INT)
                .addColumn("ck1", DataType.INT)
                .addColumn("ck2", DataType.INT)
                .addColumn("boolean_col", DataType.INT)
                .addColumn("int_col", DataType.INT)
                .addColumn("bigint_col", DataType.BIGINT)
                .addColumn("float_col", DataType.FLOAT)
                .addColumn("double_col", DataType.DOUBLE)
                .addColumn("text_col", DataType.TEXT)
                .addColumn("blob_col", DataType.BLOB)
                .addColumn("tx_id", DataType.TEXT)
                .addColumn("tx_state", DataType.INT)
                .addColumn("tx_version", DataType.INT)
                .addColumn("tx_prepared_at", DataType.BIGINT)
                .addColumn("tx_committed_at", DataType.BIGINT)
                .addColumn("before_tx_id", DataType.TEXT)
                .addColumn("before_tx_state", DataType.INT)
                .addColumn("before_tx_version", DataType.INT)
                .addColumn("before_tx_prepared_at", DataType.BIGINT)
                .addColumn("before_tx_committed_at", DataType.BIGINT)
                .addColumn("before_boolean_col", DataType.INT)
                .addColumn("before_int_col", DataType.INT)
                .addColumn("before_bigint_col", DataType.BIGINT)
                .addColumn("before_float_col", DataType.FLOAT)
                .addColumn("before_double_col", DataType.DOUBLE)
                .addColumn("before_text_col", DataType.TEXT)
                .addColumn("before_blob_col", DataType.BLOB)
                .addPartitionKey("pk")
                .addClusteringKey("ck1", Scan.Ordering.Order.ASC)
                .addClusteringKey("ck2", Scan.Ordering.Order.ASC)
                .build()
    }

    @Test
    fun `run should create a foreign table including log data for jdbc storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Jdbc(config)
        CreateForeignTables(ctx, setOf("ns_for_jdbc"), storage, admin).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_jdbc"."_jdbc_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "jdbc"
                |OPTIONS (schema_name 'ns_for_jdbc', table_name 'jdbc_table');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign table including log data for cassandra storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cassandra(config)
        CreateForeignTables(ctx, setOf("ns_for_cassandra"), storage, admin).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_cassandra"."_cassandra_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "cassandra"
                |OPTIONS (schema_name 'ns_for_cassandra', table_name 'cassandra_table');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign table including log data for cosmos storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cosmos(config)
        CreateForeignTables(ctx, setOf("ns_for_cosmos"), storage, admin).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_cosmos"."_cosmos_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "cosmos"
                |OPTIONS (namespace 'ns_for_cosmos', table_name 'cosmos_table');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign table including log data for dynamodb storage`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.DynamoDB(config)
        CreateForeignTables(ctx, setOf("ns_for_dynamodb"), storage, admin).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_dynamodb"."_dynamodb_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "dynamodb"
                |OPTIONS (namespace 'ns_for_dynamodb', table_name 'dynamodb_table');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create foreign tables for multi storage`() {
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

        CreateForeignTables(
            ctx,
            setOf(
                "ns_for_jdbc",
                "ns_for_cassandra",
                "ns_for_cosmos",
                "ns_for_dynamodb",
            ),
            storage,
            admin,
        )
            .run()

        verify {
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_jdbc"."_jdbc_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "jdbc"
                |OPTIONS (schema_name 'ns_for_jdbc', table_name 'jdbc_table');
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_cassandra"."_cassandra_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "cassandra"
                |OPTIONS (schema_name 'ns_for_cassandra', table_name 'cassandra_table');
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_cosmos"."_cosmos_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "cosmos"
                |OPTIONS (namespace 'ns_for_cosmos', table_name 'cosmos_table');
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE FOREIGN TABLE IF NOT EXISTS "ns_for_dynamodb"."_dynamodb_table" (
                |    "pk" int,
                |    "ck1" int,
                |    "ck2" int,
                |    "boolean_col" int,
                |    "int_col" int,
                |    "bigint_col" bigint,
                |    "float_col" float,
                |    "double_col" double precision,
                |    "text_col" text,
                |    "blob_col" bytea,
                |    "tx_id" text,
                |    "tx_state" int,
                |    "tx_version" int,
                |    "tx_prepared_at" bigint,
                |    "tx_committed_at" bigint,
                |    "before_tx_id" text,
                |    "before_tx_state" int,
                |    "before_tx_version" int,
                |    "before_tx_prepared_at" bigint,
                |    "before_tx_committed_at" bigint,
                |    "before_boolean_col" int,
                |    "before_int_col" int,
                |    "before_bigint_col" bigint,
                |    "before_float_col" float,
                |    "before_double_col" double precision,
                |    "before_text_col" text,
                |    "before_blob_col" bytea
                |) SERVER "dynamodb"
                |OPTIONS (namespace 'ns_for_dynamodb', table_name 'dynamodb_table');
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }
}
