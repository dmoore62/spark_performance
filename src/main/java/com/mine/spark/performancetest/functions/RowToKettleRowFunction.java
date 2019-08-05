package com.mine.spark.performancetest.functions;

import com.mine.spark.performancetest.kettle.RowMeta;
import com.mine.spark.performancetest.rows.KettleRow;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;


import java.util.Iterator;

public class RowToKettleRowFunction implements MapPartitionsFunction<Row, KettleRow> {
  RowMeta rowMeta;

  public RowToKettleRowFunction( RowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }

  @Override
  public Iterator<KettleRow> call( Iterator<Row> iterator ) throws Exception {
    return new Iterator<KettleRow>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public KettleRow next() {
        Row row = iterator.next();
        return new KettleRow( rowMeta, objects( row ) );
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
}
