package com.mine.spark.performancetest;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mine.spark.performancetest.tasks.DriverTask;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
    System.out.println( "Serializing rows into Dataset with Schema" );

    SparkSession spark = SparkSession.builder().master( "yarn" ).getOrCreate();

    Supplier<Dataset<Row>> readAction = () -> loadDefaultAction( spark );
    CompletableFuture<Dataset<Row>> result = CompletableFuture.supplyAsync( readAction, App::runOnDriver );
    result.thenApply( App::writeFiles );

    try {
      result.get();
    } catch ( InterruptedException | ExecutionException e ) {
      System.out.println( "Died here - " + e.getMessage() );
    }

    if ( triggeredFinalAction.get() ) {
      activeTasks.forEach( t -> t.cancel( false ) );
    } else {
      result.thenAcceptAsync( App::collectMetrics, App::runOnDriver );
    }
  }

  private static Dataset<Row> loadDefaultAction( SparkSession spark ) {

    StructField[] fields = new StructField[]{
      new StructField( "marketplace", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "customer_id", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "review_id", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "product_id", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "product_parent", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "product_title", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "product_category", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "star_rating", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "helpful_votes", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "total_votes", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "vine", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "verified_purchase", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "review_headline", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "review_body", DataTypes.StringType, true, Metadata.empty() ),
      new StructField( "review_date", DataTypes.StringType, true, Metadata.empty() )
    };

    StructType structType = new StructType( fields );

    return spark.read()
        .format( "org.apache.spark.csv" )
        .option( "delimiter", "\t" )
        .option( "header", true )
        .schema( structType )
        .csv( "hdfs:/user/devuser/chris/AWS/amazon_reviews_us_Apparel_v1_00.tsv" );
  }

  private static synchronized void runOnDriver( Runnable r ) {
    DriverTask dt = new DriverTask( r, null );
    activeTasks.add( dt );
    Executors.newCachedThreadPool().submit( dt );
  }

  private static void collectMetrics( Dataset<Row> output ) {
      output.count();
  }

  private static Dataset<Row> writeFiles ( Dataset<Row> output ) {
    triggeredFinalAction.set( true );
    output.write().save( FILE_OUT + "performance_test" + System.currentTimeMillis() + ".out"  );
    return output;
  }
}
