package com.scalar.db.analytics.postgresql;

public class ScalarDbFdwException extends Exception {
  public ScalarDbFdwException(String message) {
    super(message);
  }

  public ScalarDbFdwException(String message, Throwable cause) {
    super(message, cause);
  }
}
