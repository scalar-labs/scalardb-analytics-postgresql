package com.scalar.db.analytics.postgresql;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;

public class ScalarDbUtils {
  static DistributedStorage storage;
  static DistributedStorageAdmin storageAdmin;

  static void initialize(String configFilePath) throws IOException {
    // We don't need to synchronize here because only single postgres worker call
    // this from at once
    if (storage == null) {
      StorageFactory storageFactory = StorageFactory.create(configFilePath);
      storage = storageFactory.getStorage();
      storageAdmin = storageFactory.getStorageAdmin();
    }
  }

  static Scanner scan(Scan scan) throws ExecutionException {
    return storage.scan(scan);
  }

  static ScanBuilder.BuildableScan buildableScan(String namespace, String tableName, Key key) {
    return Scan.newBuilder().namespace(namespace).table(tableName).partitionKey(key);
  }

  static ScanBuilder.BuildableScanWithIndex buildableScanWithIndex(
      String namespace, String tableName, Key key) {
    return Scan.newBuilder().namespace(namespace).table(tableName).indexKey(key);
  }

  static ScanBuilder.BuildableScanAll buildableScanAll(String namespace, String tableName) {
    return Scan.newBuilder().namespace(namespace).table(tableName).all();
  }

  static Key.Builder keyBuilder() {
    return Key.newBuilder();
  }

  static int getResultColumnsSize(Result result) {
    return result.getColumns().size();
  }

  static String[] getPartitionKeyNames(String namespace, String tableName)
      throws ExecutionException, ScalarDbFdwException {
    TableMetadata metadata = storageAdmin.getTableMetadata(namespace, tableName);
    if (metadata == null) {
      throw new ScalarDbFdwException(namespace + "." + tableName + " does not exist");
    }
    return metadata.getPartitionKeyNames().toArray(new String[0]);
  }

  static String[] getClusteringKeyNames(String namespace, String tableName)
      throws ExecutionException, ScalarDbFdwException {
    TableMetadata metadata = storageAdmin.getTableMetadata(namespace, tableName);
    if (metadata == null) {
      throw new ScalarDbFdwException(namespace + "." + tableName + " does not exist");
    }
    return metadata.getClusteringKeyNames().toArray(new String[0]);
  }

  static String[] getSecondaryIndexNames(String namespace, String tableName)
      throws ExecutionException, ScalarDbFdwException {
    TableMetadata metadata = storageAdmin.getTableMetadata(namespace, tableName);
    if (metadata == null) {
      throw new ScalarDbFdwException(namespace + "." + tableName + " does not exist");
    }
    return metadata.getSecondaryIndexNames().toArray(new String[0]);
  }

  static void closeStorage() {
    if (storage != null) {
      storage.close();
      storageAdmin.close();
    }
  }
}
