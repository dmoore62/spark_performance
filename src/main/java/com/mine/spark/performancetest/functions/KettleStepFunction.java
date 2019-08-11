package com.mine.spark.performancetest.functions;

import com.mine.spark.performancetest.kettle.KettleException;
import com.mine.spark.performancetest.kettle.RowHandler;
import com.mine.spark.performancetest.kettle.RowMeta;
import com.mine.spark.performancetest.kettle.RowMetaInterface;
import com.mine.spark.performancetest.kettle.steps.Step;
import com.mine.spark.performancetest.mappers.StructTypeMapper;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class KettleStepFunction implements MapPartitionsFunction<Row, Row> {
  Step wrappedStep;
  RowMeta rowMeta;

  public KettleStepFunction( Step wrappedStep, RowMeta rowMeta ) {
    this.wrappedStep = wrappedStep;
    this.wrappedStep.init();
    this.rowMeta = rowMeta;
  }

  @Override
  public Iterator<Row> call( Iterator<Row> iterator ) throws Exception {
    Queue<Row> outputQueue = new LinkedList<>();
    StructTypeMapper structTypeMapper = new StructTypeMapper( rowMeta );
    this.wrappedStep.setRowHandler( new RowIterator( iterator, outputQueue, structTypeMapper ) );
    this.wrappedStep.setRowMeta( rowMeta );

    return new Iterator<Row>() {
      private boolean continueProcessing = true;

      @Override
      public boolean hasNext() {
        while ( outputQueue.isEmpty() && continueProcessing ) {
          try {
            continueProcessing = wrappedStep.processRow();
          } catch ( KettleException ex ) {
            continueProcessing = false;
          }
        }

        return !outputQueue.isEmpty();
      }

      @Override
      public Row next() {
        Row r = outputQueue.remove();
        return r;
      }
    };
  }

  public class RowIterator implements RowHandler {
    private final Iterator<? extends Row> input;
    private final Queue<Row> outputQueue;
    private StructTypeMapper structTypeMapper;

    public RowIterator( Iterator<? extends Row> input, Queue<Row> outputQueue, StructTypeMapper structTypeMapper ) {
      this.input = input;
      this.outputQueue = outputQueue;
      this.structTypeMapper = structTypeMapper;
    }

    @Override
    public Object[] getRow() throws KettleException {
      return input.hasNext() ? objects( input.next() ) : null;
    }

    @Override
    public void putRow( RowMetaInterface rowMeta, Object[] row ) {
      Object[] updatedValues = structTypeMapper.mapValuesTo( row );
      outputQueue.add( new GenericRowWithSchema( updatedValues, structTypeMapper.schema() ) );
    }

    @Override
    public void putError( RowMetaInterface rowMeta, Object[] row, long nrErrors, String errorDescriptions, String fieldNames, String errorCodes ) {

    }

    private Object[] objects( Row row ) {
      Object[] result = new Object[ row.size() ];
      for ( int i = 0; i < row.size(); i++ ) {
        result[ i ] = row.get( i );
      }
      return result;
    }
  }
}
