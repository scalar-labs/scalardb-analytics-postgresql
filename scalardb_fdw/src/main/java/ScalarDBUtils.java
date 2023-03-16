import java.io.IOException;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.service.StorageFactory;

public class ScalarDbUtils {
  static StorageFactory storageFactory;
  static DistributedStorage storage;

  static void initialize(String configFilePath) throws IOException {
    // We don't need to synchronize here because only single postgres worker call
    // this from at once
    if (storageFactory == null) {
      storageFactory = StorageFactory.create(configFilePath);
    }
    if (storage == null) {
      storage = storageFactory.getStorage();
    }
  }

  static Scanner scanAll(String namespace, String tableName) throws ExecutionException {
    Scan scan = Scan.newBuilder()
        .namespace(namespace)
        .table(tableName)
        .all()
        .build();
    return storage.scan(scan);
  }

  static int getResultColumnsSize(Result result) {
    return result.getColumns().size();
  }

  static void closeStorage() {
    if (storage != null) {
      storage.close();
    }
  }
}
