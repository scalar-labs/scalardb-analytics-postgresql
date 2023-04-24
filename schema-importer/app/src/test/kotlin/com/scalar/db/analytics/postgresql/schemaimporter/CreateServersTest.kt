package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.config.DatabaseConfig
import io.mockk.*
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.file.Paths
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

@ExtendWith(MockKExtension::class)
class CreateServersTest {
    @MockK(relaxUnitFun = true) lateinit var connection: Connection

    @MockK(relaxUnitFun = true) lateinit var statement: Statement

    private lateinit var ctx: DatabaseContext

    @BeforeTest
    fun setup() {
        val rs = mockk<ResultSet>()

        every { connection.createStatement() } returns statement
        every { statement.executeUpdate(any()) } returns 0
        every { statement.executeQuery(any()) } returns rs
        every { rs.next() } returns true andThen false
        every { rs.getString("setting") } returns "/path/to/postgresql/share/dir"

        ctx = DatabaseContext(connection)
    }

    @Test
    fun `run should create a foreign server for PostgreSQL using jdbc_fdw`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:postgresql://host:port/database")
        val storage = ScalarDBStorage.Jdbc(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS jdbc
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername 'org.postgresql.Driver',
                |  jarfile '/path/to/postgresql/share/dir/extension/scalardb-all.jar',
                |  url 'jdbc:postgresql://host:port/database',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign server for MySQL using jdbc_fdw`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:mysql://host:port/database")
        val storage = ScalarDBStorage.Jdbc(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS jdbc
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername 'com.mysql.jdbc.Driver',
                |  jarfile '/path/to/postgresql/share/dir/extension/scalardb-all.jar',
                |  url 'jdbc:mysql://host:port/database',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }
    @Test
    fun `run should create a foreign server for Oracle using jdbc_fdw`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:oracle:thin:@//host:port:SID")
        val storage = ScalarDBStorage.Jdbc(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS jdbc
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername 'oracle.jdbc.OracleDriver',
                |  jarfile '/path/to/postgresql/share/dir/extension/scalardb-all.jar',
                |  url 'jdbc:oracle:thin:@//host:port:SID',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign server for SQL Server using jdbc_fdw`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:sqlserver://host;DatabaseName=database")
        val storage = ScalarDBStorage.Jdbc(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS jdbc
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                |  jarfile '/path/to/postgresql/share/dir/extension/scalardb-all.jar',
                |  url 'jdbc:sqlserver://host;DatabaseName=database',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should throw IllegalArgumentException for an unsupported JDBC database type`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("jdbc:db2://host:port/database")
        val storage = ScalarDBStorage.Jdbc(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        assertFailsWith<IllegalArgumentException> { CreateServers(ctx, storage, path).run() }
    }

    @Test
    fun `run should create a foreign server for Cassandra with cassandra2_fdw`() {
        val config = mockk<DatabaseConfig>()
        every { config.contactPoints } returns listOf("localhost")
        every { config.contactPort } returns 9042
        val storage = ScalarDBStorage.Cassandra(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS cassandra
                |FOREIGN DATA WRAPPER cassandra2_fdw
                |OPTIONS (host 'localhost', port '9042');
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign server for Cosmos with scalardb_fdw`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cosmos(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS cosmos
                |FOREIGN DATA WRAPPER scalardb_fdw
                |OPTIONS (config_file_path '/absolute/path/to/config.properties');
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign server for DynamoDB with scalardb_fdw`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.DynamoDB(config)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS dynamodb
                |FOREIGN DATA WRAPPER scalardb_fdw
                |OPTIONS (config_file_path '/absolute/path/to/config.properties');
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create multiple foreign servers for multi storage`() {
        val jdbcConfig = mockk<DatabaseConfig>()
        every { jdbcConfig.contactPoints } returns listOf("jdbc:postgresql://host:port/database")

        val cassandraConfig = mockk<DatabaseConfig>()
        every { cassandraConfig.contactPoints } returns listOf("localhost")
        every { cassandraConfig.contactPort } returns 9042

        val storages =
            mapOf(
                "jdbc" to ScalarDBStorage.Jdbc(jdbcConfig),
                "cassandra" to ScalarDBStorage.Cassandra(cassandraConfig),
            )

        val namespaceStorageMap =
            mapOf(
                "ns_for_jdbc" to "jdbc",
                "ns_for_cassandra" to "cassandra",
            )

        val storage = ScalarDBStorage.MultiStorage(storages, namespaceStorageMap)
        val path = Paths.get("/absolute/path/to/config.properties")

        CreateServers(ctx, storage, path).run()

        verify {
            statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS jdbc
                |FOREIGN DATA WRAPPER jdbc_fdw
                |OPTIONS (
                |  drivername 'org.postgresql.Driver',
                |  jarfile '/path/to/postgresql/share/dir/extension/scalardb-all.jar',
                |  url 'jdbc:postgresql://host:port/database',
                |  querytimeout '60',
                |  maxheapsize '1024'
                |);
                """
                    .trimMargin()
            )
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS cassandra
                |FOREIGN DATA WRAPPER cassandra2_fdw
                |OPTIONS (host 'localhost', port '9042');
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }

    @Test
    fun `run should create a foreign server of scalardb_fdw with the path specified as --config-path-on-postgres-host`() {
        val config = mockk<DatabaseConfig>()
        val storage = ScalarDBStorage.Cosmos(config)
        val path = Paths.get("/absolute/path/to/config.properties")
        val configPathOnPostgresHost = Paths.get("/absolute/path/on/postgres/host/to/config.properties")

        CreateServers(ctx, storage, path, configPathOnPostgresHost).run()

        verify {
            statement.executeUpdate(
                """
                |CREATE SERVER IF NOT EXISTS cosmos
                |FOREIGN DATA WRAPPER scalardb_fdw
                |OPTIONS (config_file_path '/absolute/path/on/postgres/host/to/config.properties');
                """
                    .trimMargin()
            )
            statement.close()
        }
        confirmVerified(statement)
    }
}
