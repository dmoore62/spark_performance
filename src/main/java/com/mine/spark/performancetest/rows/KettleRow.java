package com.mine.spark.performancetest.rows;

import com.sun.rowset.internal.Row;

import java.util.List;

public class KettleRow {
  private Object[] values;
  private List<String> headers;

  public KettleRow( Object[] values, List<String> headers ) {
    this.values = values;
    this.headers = headers;
  }

  public Object[] getValues() {
    return values;
  }

  public List<String> getHeaders() {
    return headers;
  }
}
