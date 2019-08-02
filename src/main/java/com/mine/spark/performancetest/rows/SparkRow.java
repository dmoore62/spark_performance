package com.mine.spark.performancetest.rows;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

public class SparkRow implements Row {
  private Row row;

  public SparkRow( Object[] values ) {
    this.row = new GenericRow( values );
  }

  public int size() {
    return row.size();
  }

  public int length() {
    return row.length();
  }

  public StructType schema() {
    return row.schema();
  }

  public Object apply( int i ) {
    return row.apply( i );
  }

  public Object get( int i ) {
    return null;
  }

  public boolean isNullAt( int i ) {
    return row.isNullAt( i );
  }

  public boolean getBoolean( int i ) {
    return row.getBoolean( i );
  }

  public byte getByte( int i ) {
    return row.getByte( i );
  }

  public short getShort( int i ) {
    return row.getShort( i );
  }

  public int getInt( int i ) {
    return row.getInt( i );
  }

  public long getLong( int i ) {
    return row.getLong( i );
  }

  public float getFloat( int i ) {
    return row.getFloat( i );
  }

  public double getDouble( int i ) {
    return row.getDouble( i );
  }

  public String getString( int i ) {
    return row.getString( i );
  }

  public BigDecimal getDecimal( int i ) {
    return row.getDecimal( i );
  }

  public Date getDate( int i ) {
    return row.getDate( i );
  }

  public Timestamp getTimestamp( int i ) {
    return row.getTimestamp( i );
  }

  public <T> Seq<T> getSeq( int i ) {
    return row.getSeq( i );
  }

  public <T> List<T> getList( int i ) {
    return row.getList( i );
  }

  public <K, V> Map<K, V> getMap( int i ) {
    return row.getMap( i );
  }

  public <K, V> java.util.Map<K, V> getJavaMap( int i ) {
    return row.getJavaMap( i );
  }

  public Row getStruct( int i ) {
    return row.getStruct( i );
  }

  public <T> T getAs( int i ) {
    return row.getAs( i );
  }

  public <T> T getAs( String fieldName ) {
    return row.getAs( fieldName );
  }

  public int fieldIndex( String name ) {
    return row.fieldIndex( name );
  }

  public <T> scala.collection.immutable.Map<String, T> getValuesMap( Seq<String> fieldNames ) {
    return row.getValuesMap( fieldNames );
  }

  public Row copy() {
    return null;
  }

  public boolean anyNull() {
    return row.anyNull();
  }

  public Seq<Object> toSeq() {
    return row.toSeq();
  }

  public String mkString() {
    return row.mkString();
  }

  public String mkString( String sep ) {
    return row.mkString( sep );
  }

  public String mkString( String start, String sep, String end ) {
    return row.mkString( start, sep, end );
  }
}
