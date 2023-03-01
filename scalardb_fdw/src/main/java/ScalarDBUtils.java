import java.io.IOException;
import java.util.List;

import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Result;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.api.Scan;
import com.scalar.db.service.TransactionFactory;

public class ScalarDBUtils {
  static TransactionFactory transactionFactory;
  static DistributedTransactionManager transactionManager;

  static void initialize(String configFilePath) throws IOException {
    // We don't need to synchronize here because only single postgres worker call
    // this from at once
    if (transactionFactory == null) {
      transactionFactory = TransactionFactory.create(configFilePath);
    }
    if (transactionManager == null) {
      transactionManager = transactionFactory.getTransactionManager();
    }
  }

  static DistributedTransaction beginTransaction() throws TransactionException {
    return transactionManager.begin();
  }

  static List<Result> scanAll(DistributedTransaction transaction, String namespace, String tableName)
      throws TransactionException {
    Scan scan = Scan.newBuilder().namespace(namespace).table(tableName).all().build();
    return transaction.scan(scan);
  }

  static int getResultColumnsSize(Result result) {
    return result.getColumns().size();
  }

  static void closeTransactionManager() {
    if (transactionManager != null) {
      transactionManager.close();
    }
  }
}
