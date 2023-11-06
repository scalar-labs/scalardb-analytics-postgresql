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
package com.scalar.db.analytics.postgresql;

import com.scalar.db.api.*;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDataLoader {
  private static final Logger logger = LoggerFactory.getLogger(TestDataLoader.class);

  private static final String CASSANDRA_NAMESPACE = "cassandrans";
  private static final String CASSANDRA_TABLE_TEST = "test";

  private static final String POSTGRES_NAMESPACE = "postgresns";
  private static final String POSTGRES_TABLE_TEST = "test";
  private static final String POSTGRES_TABLE_NULL_TEST = "null_test";
  private static final String BOOLEAN_TEST_TABLE = "boolean_test";
  private static final String INT_TEST_TABLE = "int_test";
  private static final String BIGINT_TEST_TABLE = "bigint_test";
  private static final String FLOAT_TEST_TABLE = "float_test";
  private static final String DOUBLE_TEST_TABLE = "double_test";
  private static final String TEXT_TEST_TABLE = "text_test";
  private static final String BLOB_TEST_TABLE = "blob_test";

  public static void main(String... args) {
    if (args.length != 1) {
      logger.error("Usage: java -jar test-data-loader.jar <propertiesPath>");
      System.exit(1);
    }

    Path propertiesPath = Paths.get(args[0]);
    try {
      TransactionFactory factory = TransactionFactory.create(propertiesPath);
      createTables(factory);
      loadTestData(factory);
    } catch (Exception e) {
      logger.error("Failed to load test data", e);
      System.exit(1);
    }
    logger.info("Test data loaded successfully");
    System.exit(0);
  }

  private static void createTables(TransactionFactory factory) throws ExecutionException {
    DistributedTransactionAdmin admin = factory.getTransactionAdmin();
    try {
      logger.info("Creating coordinator tables and namespaces");
      admin.createCoordinatorTables(true);
      admin.createNamespace(CASSANDRA_NAMESPACE, true);
      admin.createNamespace(POSTGRES_NAMESPACE, true);

      createCassandraTestTable(admin);
      createPostgresTestTable(admin);
      createPostgresNullTestTable(admin);
      createBooleanTestTable(admin);
      createIntTestTable(admin);
      createBigIntTestTable(admin);
      createFloatTestTable(admin);
      createDoubleTestTable(admin);
      createTextTestTable(admin);
      createBlobTestTable(admin);
    } finally {
      admin.close();
    }
  }

  private static void createCassandraTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(CASSANDRA_NAMESPACE, CASSANDRA_TABLE_TEST)) {
      logger.info("cassandrans.test already exists. Truncating it");
      admin.truncateTable(CASSANDRA_NAMESPACE, CASSANDRA_TABLE_TEST);
      return;
    }

    logger.info("Creating cassandrans.test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c_pk", DataType.INT)
            .addColumn("c_ck1", DataType.INT)
            .addColumn("c_ck2", DataType.INT)
            .addColumn("c_boolean_col", DataType.BOOLEAN)
            .addColumn("c_int_col", DataType.INT)
            .addColumn("c_bigint_col", DataType.BIGINT)
            .addColumn("c_float_col", DataType.FLOAT)
            .addColumn("c_double_col", DataType.DOUBLE)
            .addColumn("c_text_col", DataType.TEXT)
            .addColumn("c_blob_col", DataType.BLOB)
            .addPartitionKey("c_pk")
            .addClusteringKey("c_ck1")
            .addClusteringKey("c_ck2")
            .build();
    admin.createTable(CASSANDRA_NAMESPACE, CASSANDRA_TABLE_TEST, tableMetadata, true);
    admin.truncateTable(CASSANDRA_NAMESPACE, CASSANDRA_TABLE_TEST);
  }

  private static void createPostgresTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, POSTGRES_TABLE_TEST)) {
      logger.info("postgresns.test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, POSTGRES_TABLE_TEST);
      return;
    }

    logger.info("Creating postgresns.test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("p_pk", DataType.INT)
            .addColumn("p_ck1", DataType.INT)
            .addColumn("p_ck2", DataType.INT)
            .addColumn("p_boolean_col", DataType.BOOLEAN)
            .addColumn("p_int_col", DataType.INT)
            .addColumn("p_bigint_col", DataType.BIGINT)
            .addColumn("p_float_col", DataType.FLOAT)
            .addColumn("p_double_col", DataType.DOUBLE)
            .addColumn("p_text_col", DataType.TEXT)
            .addColumn("p_blob_col", DataType.BLOB)
            .addPartitionKey("p_pk")
            .addClusteringKey("p_ck1")
            .addClusteringKey("p_ck2")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, POSTGRES_TABLE_TEST, tableMetadata, true);
  }

  private static void createPostgresNullTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, POSTGRES_TABLE_NULL_TEST)) {
      logger.info("postgresns.null_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, POSTGRES_TABLE_NULL_TEST);
      return;
    }

    logger.info("Creating postgresns.null_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("p_pk", DataType.INT)
            .addColumn("p_ck1", DataType.INT)
            .addColumn("p_ck2", DataType.INT)
            .addColumn("p_boolean_col", DataType.BOOLEAN)
            .addColumn("p_int_col", DataType.INT)
            .addColumn("p_bigint_col", DataType.BIGINT)
            .addColumn("p_float_col", DataType.FLOAT)
            .addColumn("p_double_col", DataType.DOUBLE)
            .addColumn("p_text_col", DataType.TEXT)
            .addColumn("p_blob_col", DataType.BLOB)
            .addPartitionKey("p_pk")
            .addClusteringKey("p_ck1")
            .addClusteringKey("p_ck2")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, POSTGRES_TABLE_NULL_TEST, tableMetadata, true);
  }

  private static void createBooleanTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, BOOLEAN_TEST_TABLE)) {
      logger.info("postgresns.boolean_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, BOOLEAN_TEST_TABLE);
      return;
    }

    DataType type = DataType.BOOLEAN;

    logger.info("Creating postgresns.boolean_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, BOOLEAN_TEST_TABLE, tableMetadata, true);
  }

  private static void createIntTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, INT_TEST_TABLE)) {
      logger.info("postgresns.int_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, INT_TEST_TABLE);
      return;
    }

    DataType type = DataType.INT;

    logger.info("Creating postgresns.int_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, INT_TEST_TABLE, tableMetadata, true);
  }

  private static void createBigIntTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, BIGINT_TEST_TABLE)) {
      logger.info("postgresns.bigint_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, BIGINT_TEST_TABLE);
      return;
    }

    DataType type = DataType.BIGINT;

    logger.info("Creating postgresns.bigint_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, BIGINT_TEST_TABLE, tableMetadata, true);
  }

  private static void createFloatTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, FLOAT_TEST_TABLE)) {
      logger.info("postgresns.float_tst already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, FLOAT_TEST_TABLE);
      return;
    }

    DataType type = DataType.FLOAT;

    logger.info("Creating postgresns.float_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, FLOAT_TEST_TABLE, tableMetadata, true);
  }

  private static void createDoubleTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, DOUBLE_TEST_TABLE)) {
      logger.info("postgresns.double_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, DOUBLE_TEST_TABLE);
      return;
    }

    DataType type = DataType.DOUBLE;

    logger.info("Creating postgresns.double_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, DOUBLE_TEST_TABLE, tableMetadata, true);
  }

  private static void createTextTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, TEXT_TEST_TABLE)) {
      logger.info("postgresns.text_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, TEXT_TEST_TABLE);
      return;
    }

    DataType type = DataType.TEXT;

    logger.info("Creating postgresns.text_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, TEXT_TEST_TABLE, tableMetadata, true);
  }

  private static void createBlobTestTable(DistributedTransactionAdmin admin)
      throws ExecutionException {
    if (admin.tableExists(POSTGRES_NAMESPACE, BLOB_TEST_TABLE)) {
      logger.info("postgresns.blob_test already exists. Truncating it");
      admin.truncateTable(POSTGRES_NAMESPACE, BLOB_TEST_TABLE);
      return;
    }

    DataType type = DataType.BLOB;

    logger.info("Creating postgresns.blob_test table");
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("pk", type)
            .addColumn("ck", type)
            .addColumn("index", type)
            .addColumn("col", type)
            .addPartitionKey("pk")
            .addClusteringKey("ck")
            .addSecondaryIndex("index")
            .build();
    admin.createTable(POSTGRES_NAMESPACE, BLOB_TEST_TABLE, tableMetadata, true);
  }

  private static void loadTestData(TransactionFactory factory) throws TransactionException {
    DistributedTransactionManager manager = factory.getTransactionManager();
    DistributedTransaction tx = manager.start();
    try {
      loadCassandraTestData(tx);
      loadPostgresTestData(tx);
      loadPostgresNullTestData(tx);
      tx.commit();
    } catch (CrudException | CommitException e) {
      tx.rollback();
      throw e;
    }
  }

  private static void loadCassandraTestData(DistributedTransaction tx) throws CrudException {
    logger.info("Loading cassandrans.test table data");
    Put put =
        Put.newBuilder()
            .namespace(CASSANDRA_NAMESPACE)
            .table(CASSANDRA_TABLE_TEST)
            .partitionKey(Key.ofInt("c_pk", 1))
            .clusteringKey(Key.of("c_ck1", 1, "c_ck2", 1))
            .booleanValue("c_boolean_col", true)
            .intValue("c_int_col", 1)
            .bigIntValue("c_bigint_col", 1L)
            .floatValue("c_float_col", 1.0f)
            .doubleValue("c_double_col", 1.0)
            .textValue("c_text_col", "test")
            .blobValue("c_blob_col", new byte[] {1, 2, 3})
            .build();
    tx.put(put);
  }

  private static void loadPostgresTestData(DistributedTransaction tx) throws CrudException {
    logger.info("Loading postgresns.test table data");
    Put put =
        Put.newBuilder()
            .namespace(POSTGRES_NAMESPACE)
            .table(POSTGRES_TABLE_TEST)
            .partitionKey(Key.ofInt("p_pk", 1))
            .clusteringKey(Key.of("p_ck1", 1, "p_ck2", 1))
            .booleanValue("p_boolean_col", true)
            .intValue("p_int_col", 1)
            .bigIntValue("p_bigint_col", 1L)
            .floatValue("p_float_col", 1.0f)
            .doubleValue("p_double_col", 1.0)
            .textValue("p_text_col", "test")
            .blobValue("p_blob_col", new byte[] {1, 2, 3})
            .build();
    tx.put(put);
  }

  private static void loadPostgresNullTestData(DistributedTransaction tx) throws CrudException {
    logger.info("Loading postgresns.null_test table data");
    Put put =
        Put.newBuilder()
            .namespace(POSTGRES_NAMESPACE)
            .table(POSTGRES_TABLE_NULL_TEST)
            .partitionKey(Key.ofInt("p_pk", 1))
            .clusteringKey(Key.of("p_ck1", 1, "p_ck2", 1))
            .build();
    tx.put(put);
  }
}
