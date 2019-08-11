package com.mine.spark.performancetest.kettle.steps;

import com.mine.spark.performancetest.kettle.KettleException;
import com.mine.spark.performancetest.kettle.RowHandler;
import com.mine.spark.performancetest.kettle.RowMeta;
import com.mine.spark.performancetest.kettle.ValueMetaString;

import java.io.Serializable;

public class ConcatStep implements Step, Serializable {
  RowHandler rowHandler;
  RowMeta rowMeta;

  public ConcatStep() {
  }

  @Override
  public boolean init() {
    return false;
  }

  @Override
  public boolean processRow() throws KettleException {
    Object[] r = getRow();

    Object[] newRow = new Object[r.length + 1];

    for ( int i = 0; i < r.length; i ++ ) {
      newRow[i] = r[i];
    }

    String newVal = newRow[0].toString() + newRow[1];

    newRow[newRow.length - 1] = newVal;

    rowMeta.addValueMeta( newRow.length - 1, new ValueMetaString( "concat_field" ) );

    putRow( newRow );

    return true;
  }

  @Override
  public void putRow( Object[] data ) {
    rowHandler.putRow( rowMeta, data );
  }

  @Override
  public Object[] getRow() throws KettleException {
    return rowHandler.getRow();
  }

  public void setRowHandler( RowHandler rowHandler ) {
    this.rowHandler = rowHandler;
  }

  public void setRowMeta( RowMeta rowMeta ) {
    this.rowMeta = rowMeta;
  }
}
