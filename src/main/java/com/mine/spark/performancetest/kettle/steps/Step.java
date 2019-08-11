package com.mine.spark.performancetest.kettle.steps;

import com.mine.spark.performancetest.kettle.KettleException;
import com.mine.spark.performancetest.kettle.RowHandler;
import com.mine.spark.performancetest.kettle.RowMeta;

public interface Step {
  boolean init();

  boolean processRow() throws KettleException;

  void putRow( Object[] data );

  Object[] getRow() throws KettleException;

  void setRowHandler( RowHandler rowHandler );

  void setRowMeta( RowMeta rowMeta );
}
