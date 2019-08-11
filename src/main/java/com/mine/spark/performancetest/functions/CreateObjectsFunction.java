package com.mine.spark.performancetest.functions;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.util.Iterator;

public class CreateObjectsFunction implements MapPartitionsFunction<Row, Row> {
  private int number;

  public CreateObjectsFunction( int number ) {
    this.number = number;
  }

  @Override
  public Iterator<Row> call( Iterator<Row> iterator ) throws Exception {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Row next() {
        Row r = iterator.next();
        for ( int i = 0; i < number; i ++ ) {
          Object o = new String( "dumbObject" );
          doSomethingWith( o );
        }
        return new GenericRowWithSchema( objects( r ), r.schema() );
      }

      private Object[] objects( Row row ) {
        Object[] result = new Object[ row.size() ];
        for ( int i = 0; i < row.size(); i++ ) {
          result[ i ] = row.get( i );
        }
        return result;
      }
    };
  }

  private void doSomethingWith( Object o ) {
    try {
      Thread.currentThread().wait( 300 );
      o.toString();
    } catch ( InterruptedException ex ) {
      //doNothing
    }
  }
}
