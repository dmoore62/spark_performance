package com.mine.spark.performancetest.tasks;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

public class DriverTask extends FutureTask<Void> {
  public DriverTask( Callable<Void> callable ) {
    super( callable );
  }

  public DriverTask( Runnable runnable, Void result ) {
    super( runnable, null );
  }
}
