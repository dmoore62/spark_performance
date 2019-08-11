package com.mine.spark.performancetest.kettle;

public interface RowHandler {
  Object[] getRow() throws KettleException;

  void putRow( RowMetaInterface rowMeta, Object[] row );

  void putError( RowMetaInterface rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                 String fieldNames, String errorCodes );
}
