package com.mine.spark.performancetest.functions;

import com.mine.spark.performancetest.kettle.RowMeta;
import com.mine.spark.performancetest.mappers.StructTypeMapper;
import com.mine.spark.performancetest.rows.KettleRow;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;


import java.util.Iterator;

public class RowToKettleRowFunction implements MapPartitionsFunction<Row, Row> {
  RowMeta rowMeta;
  StructType structType;

  public RowToKettleRowFunction( RowMeta rowMeta ) {
    this.rowMeta = rowMeta;
    this.structType = new StructTypeMapper( rowMeta ).schema();
  }

  @Override
  public Iterator<Row> call( Iterator<Row> iterator ) throws Exception {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public KettleRow next() {
        Row row = iterator.next();
        return new KettleRow( structType, objects( row ) );
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
