package com.skt.metatron.modelmanager.executor.exception;

import scala.tools.nsc.Global;

/**
 * Created by kyungtaak on 2016. 10. 3..
 */
public class ExecutorException extends RuntimeException {

  public ExecutorException() {
  }

  public ExecutorException(String message) {
    super(message);
  }

  public ExecutorException(String message, Throwable cause) {
    super(message, cause);
  }

  public ExecutorException(Throwable cause) {
    super(cause);
  }

  public ExecutorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
