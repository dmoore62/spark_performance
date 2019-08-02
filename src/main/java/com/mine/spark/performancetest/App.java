package com.mine.spark.performancetest;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mine.spark.performancetest.tasks.DriverTask;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class App {
  static final String FILE_OUT = "hdfs:/user/dmoore/out/";
  static AtomicBoolean triggeredFinalAction = new AtomicBoolean( false );
  static Set<DriverTask> activeTasks = Sets.newSetFromMap( Maps.newConcurrentMap() );

  public static void main( String[] args ) {
    System.out.println( "Hello World" );

    SparkConf sparkConf = new SparkConf().setMaster( "yarn" );
    SparkContext sc = new SparkContext( sparkConf );

    Supplier<RDD<String>> readAction = () -> loadDefaultAction( sc );
    CompletableFuture<RDD<String>> result = CompletableFuture.supplyAsync( readAction, App::runOnDriver );
    result.thenApply( App::writeFiles );
    result.thenAcceptAsync( App::collectMetrics, App::runOnDriver );

    try {
      result.get();
    } catch ( InterruptedException | ExecutionException e ) {
      System.out.println( "Died here - " + e.getMessage() );
    }
  }

  private static RDD<String> loadDefaultAction( SparkContext sc ) {
    triggeredFinalAction.set( true );
    return sc.textFile( "hdfs:/user/devuser/chris/AWS/amazon_reviews_us_Apparel_v1_00.tsv", 1 );
  }

  private static synchronized void runOnDriver( Runnable r ) {
    DriverTask dt = new DriverTask( r, null );
    activeTasks.add( dt );
    if ( triggeredFinalAction.get() ) {
      activeTasks.forEach( t -> t.cancel( false ) );
    } else {
      Executors.newCachedThreadPool().submit( dt );
    }
  }

  private static void collectMetrics( RDD<String> output ) {
      output.count();
  }

  private static RDD<String> writeFiles ( RDD<String> output ) {
    output.saveAsTextFile( FILE_OUT + "performance_test" + System.currentTimeMillis() + ".out" );
    return output;
  }
}
