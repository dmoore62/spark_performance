package com.mine.spark.performancetest;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mine.spark.performancetest.functions.CreateObjectsFunction;
import com.mine.spark.performancetest.functions.KettleStepFunction;
import com.mine.spark.performancetest.functions.RowToKettleRowFunction;
import com.mine.spark.performancetest.kettle.RowMeta;
import com.mine.spark.performancetest.kettle.ValueMetaString;
import com.mine.spark.performancetest.kettle.steps.ConcatStep;
import com.mine.spark.performancetest.kettle.steps.Step;
import com.mine.spark.performancetest.mappers.StructTypeMapper;
import com.mine.spark.performancetest.rows.KettleRow;
import com.mine.spark.performancetest.tasks.DriverTask;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
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
  static final Logger LOG = LogManager.getLogger( App.class );
  static AtomicBoolean triggeredFinalAction = new AtomicBoolean( false );
  //static Set<DriverTask> activeTasks = Sets.newSetFromMap( Maps.newConcurrentMap() );
  static StructType structType;
  private static String readFile;
  private static String writeFile;
  private static SparkSession spark;

  public static void main( String[] args ) {
    verifyArgs( args );
    System.out.println( "Serializing rows into Dataset with Schema" );

    spark = SparkSession.builder().getOrCreate();

    //Multi-thread it
//    Supplier<Dataset<Row>> readAction = () -> loadDefaultAction();
//    CompletableFuture<Dataset<Row>> result =
//        CompletableFuture.supplyAsync( readAction, App::runOnDriver )
//            .thenApply( App::convertToKettleRow )
//            .thenApply( App::createObjectsForNoReason )
//            .thenApply( App::concatFieldsInWrappedFunction )
//            .thenApply( App::writeFiles )
//            .exceptionally( App::throwSomething );

    //Simple
    //Dataset<Row> ds = loadDefaultAction();
    //ds = concatFieldsInWrappedFunction( ds );
    //writeFiles( ds );

    //Basic Dataset
//    Dataset<Row> ds = spark.read()
//        .format( "org.apache.spark.csv" )
//        .option( "delimiter", "\t" )
//        .option( "header", true )
//        .option( "mode", "DROPMALFORMED")
//        .schema( new StructType( getFields() ) )
//        .csv( readFile );
//
//    ds.write()
//        .option( "header", true )
//        .csv( writeFile + "performance_test" + System.currentTimeMillis() + ".out"  );

    //Basic RDD
    RDD<String> rdd = spark.sparkContext().textFile( readFile, 1 );
    rdd.saveAsTextFile( writeFile + "performance_test" + System.currentTimeMillis() + ".out" );

//    try {
//      result.get();
//    } catch ( InterruptedException | ExecutionException e ) {
//      System.out.println( "Died here - " + e.getMessage() );
//    }

//    if ( triggeredFinalAction.get() ) {
//      //activeTasks.forEach( t -> t.cancel( false ) );
//    } else {
//      //result.thenAcceptAsync( App::collectMetrics, App::runOnDriver );
//      collectMetrics( ds );
//    }
  }

  private static Dataset<Row> throwSomething( Throwable throwable ) {
    LOG.debug( "<><><><<><><><> Bombed it !!!!! <><><<><<", throwable );
    return spark.emptyDataset( Encoders.javaSerialization( Row.class ) );
  }

  private static void verifyArgs( String[] args ) {
    if( args.length < 2 ) {
      System.out.println( "Needs read/write files" );
      System.exit( 1 );
    } else {
      readFile = args[0];
      writeFile = args[1];
    }
  }

  private static Dataset<Row> loadDefaultAction() {
    structType = new StructType( getFields() );

    return spark.read()
        .format( "org.apache.spark.csv" )
        .option( "delimiter", "\t" )
        .option( "header", true )
        .option( "mode", "DROPMALFORMED")
        .schema( structType )
        .csv( readFile );
  }

//  private static synchronized void runOnDriver( Runnable r ) {
//    DriverTask dt = new DriverTask( r, null );
//    activeTasks.add( dt );
//    Executors.newCachedThreadPool().submit( dt );
//  }

  private static void collectMetrics( Dataset<Row> output ) {
      output.count();
  }

  private static Dataset<Row> writeFiles ( Dataset<Row> output ) {
    //triggeredFinalAction.set( true );
    output
        //.limit( 50 )
        .write()
        .option( "header", true )
        .csv( writeFile + "performance_test" + System.currentTimeMillis() + ".out"  );
    return output;
  }

  private static Dataset<Row> convertToKettleRow( Dataset<Row> output ) {
    RowMeta rowMeta = getRowMeta();

    //This function will map everything to new objects using a single structTypeMapper
    MapPartitionsFunction<Row, Row> function = new RowToKettleRowFunction( rowMeta );

    return output.mapPartitions( function, Encoders.javaSerialization( Row.class ) );
  }

  private static Dataset<Row> createObjectsForNoReason( Dataset<Row> output ) {
    MapPartitionsFunction<Row, Row> function = new CreateObjectsFunction( 100000 );
    return output.mapPartitions( function, RowEncoder.apply( structType ) );
  }

  private static Dataset<Row> concatFieldsInWrappedFunction( Dataset<Row> output ) {
    Step concatFields = new ConcatStep();
    RowMeta rowMeta = getRowMeta();
    rowMeta.addValueMeta( rowMeta.size(), new ValueMetaString( "concat_fields" ) );
    KettleStepFunction function = new KettleStepFunction( concatFields, rowMeta );

    StructType structType = new StructTypeMapper( rowMeta ).schema();
    return output.mapPartitions( function, RowEncoder.apply( structType ) );
  }

  private static RowMeta getRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( 0, new ValueMetaString( "marketplace" ) );
    rowMeta.addValueMeta( 1, new ValueMetaString( "customer_id" ) );
    rowMeta.addValueMeta( 2, new ValueMetaString( "review_id" ) );
    rowMeta.addValueMeta( 3, new ValueMetaString( "product_id" ) );
    rowMeta.addValueMeta( 4, new ValueMetaString( "product_parent" ) );
    rowMeta.addValueMeta( 5, new ValueMetaString( "product_title" ) );
    rowMeta.addValueMeta( 6, new ValueMetaString( "product_category" ) );
    rowMeta.addValueMeta( 7, new ValueMetaString( "star_rating" ) );
    rowMeta.addValueMeta( 8, new ValueMetaString( "helpful_votes" ) );
    rowMeta.addValueMeta( 9, new ValueMetaString( "total_votes" ) );
    rowMeta.addValueMeta( 10, new ValueMetaString( "vine" ) );
    rowMeta.addValueMeta( 11, new ValueMetaString( "verified_purchase" ) );
    rowMeta.addValueMeta( 12, new ValueMetaString( "review_headline" ) );
    rowMeta.addValueMeta( 13, new ValueMetaString( "review_body" ) );
    rowMeta.addValueMeta( 14, new ValueMetaString( "review_date" ) );

    return rowMeta;
  }

  private static StructField[] getFields() {
    return new StructField[]{
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
  }
}
