package com.mine.spark.performancetest.functions;

import com.mine.spark.performancetest.kettle.steps.Step;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;

public class KettleStepFunction implements MapPartitionsFunction<Row, Row> {
  @Override
  public Iterator<Row> call( Iterator<Row> iterator ) throws Exception {
    return null;
  }
}
