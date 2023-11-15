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
import com.scalar.db.io.DataType
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
class CreateViewsTest {
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
    fun `run should create view for jdbc storage`() {
        CreateViews(ctx, setOf("ns_for_jdbc"), admin).run()
        verify {
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_jdbc"."jdbc_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_jdbc"."_jdbc_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create view for cassandra storage`() {
        CreateViews(ctx, setOf("ns_for_cassandra"), admin).run()
        verify {
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_cassandra"."cassandra_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_cassandra"."_cassandra_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create view for cosmos storage`() {
        CreateViews(ctx, setOf("ns_for_cosmos"), admin).run()
        verify {
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_cosmos"."cosmos_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_cosmos"."_cosmos_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create view for dynamodb storage`() {
        CreateViews(ctx, setOf("ns_for_dynamodb"), admin).run()
        verify {
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_dynamodb"."dynamodb_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_dynamodb"."_dynamodb_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create required views for multi storage`() {
        CreateViews(
            ctx,
            setOf(
                "ns_for_jdbc",
                "ns_for_cassandra",
                "ns_for_cosmos",
                "ns_for_dynamodb",
            ),
            admin,
        )
            .run()
        verify {
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_jdbc"."jdbc_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_jdbc"."_jdbc_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_cassandra"."cassandra_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_cassandra"."_cassandra_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_cosmos"."cosmos_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_cosmos"."_cosmos_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.executeUpdate(
                """
                |CREATE OR REPLACE VIEW "ns_for_dynamodb"."dynamodb_table" AS
                |SELECT
                |    "pk",
                |    "ck1",
                |    "ck2",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "boolean_col" ELSE "before_boolean_col" END AS "boolean_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "int_col" ELSE "before_int_col" END AS "int_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "bigint_col" ELSE "before_bigint_col" END AS "bigint_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "float_col" ELSE "before_float_col" END AS "float_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "double_col" ELSE "before_double_col" END AS "double_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "text_col" ELSE "before_text_col" END AS "text_col",
                |    CASE WHEN "tx_state" = 3 OR "tx_state" IS NULL THEN "blob_col" ELSE "before_blob_col" END AS "blob_col"
                |FROM "ns_for_dynamodb"."_dynamodb_table"
                |WHERE "tx_state" = 3 OR "tx_state" IS NULL OR "before_tx_state" = 3;
                """
                    .trimMargin(),
            )
            statement.close()
        }
        confirmVerified(statement)
    }
}
