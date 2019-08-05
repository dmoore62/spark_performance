package com.mine.spark.performancetest.rows;

import com.mine.spark.performancetest.kettle.RowMetaInterface;
import com.mine.spark.performancetest.mappers.StructTypeMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import org.pentaho.di.engine.spark.spi.RowUtil;

import scala.collection.Map;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public class KettleRow implements Row {

  private static final long serialVersionUID = 6223339699756456084L;

  private final GenericRowWithSchema row;

  public KettleRow( StructType schema, Object[] values ) {
    this.row = new GenericRowWithSchema( values, schema );
  }

  @Deprecated
  public KettleRow( RowMetaInterface rowMeta, Object[] values ) {
    this( rowMeta, values, false );
  }

  @Deprecated
  public KettleRow( RowMetaInterface rowMeta, Object[] values, boolean convert ) {
    StructTypeMapper mapper = new StructTypeMapper( rowMeta );
    StructType schema = mapper.schema();
    if ( convert ) {
      Object[] updatedValues = mapper.mapValuesTo( values );
      this.row = new GenericRowWithSchema( updatedValues, schema );
    } else {
      this.row = new GenericRowWithSchema( values, schema );
    }
  }

  /**
   * TODO ccaspanello should this delegate to row method.
   *
   * @param o
   * @return
   */
  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }

    org.pentaho.di.engine.spark.spi.KettleRow kettleRow = (org.pentaho.di.engine.spark.spi.KettleRow) o;

    //only need to compare the values list up to the size of the names list.
    //sometimes the values list gets over-allocated on purpose for efficiency adding in new values
    Object[] values = RowUtil.getObjects( row );
    List<String> names = RowUtil.getColumnNames( row );
    return
        asList( values ).subList( 0, names.size() )
            .equals( asList( RowUtil.getObjects( kettleRow ) ).subList( 0, names.size() ) );
  }

  @Override public int hashCode() {
    Object[] values = RowUtil.getObjects( row );
    List<String> names = RowUtil.getColumnNames( row );
    return Objects.hash( values, names );
  }

  @Override public Seq<Object> toSeq() {
    return row.toSeq();
  }

  @Override public String mkString() {
    return row.mkString();
  }

  @Override public String mkString( String sep ) {
    return row.mkString( sep );
  }

  @Override public String mkString( String start, String sep, String end ) {
    return row.mkString( start, sep, end );
  }

  @Override public int size() {
    return row.size();
  }

  @Override public int length() {
    return row.length();
  }

  @Override public StructType schema() {
    return row.schema();
  }

  @Override public Object apply( int i ) {
    return row.apply( i );
  }

  @Override public Object get( int i ) {
    return row.get( i );
  }

  @Override public boolean isNullAt( int i ) {
    return row.isNullAt( i );
  }

  @Override public boolean getBoolean( int i ) {
    return row.getBoolean( i );
  }

  @Override public byte getByte( int i ) {
    return row.getByte( i );
  }

  @Override public short getShort( int i ) {
    return row.getShort( i );
  }

  @Override public int getInt( int i ) {
    return row.getInt( i );
  }

  @Override public long getLong( int i ) {
    return row.getLong( i );
  }

  @Override public float getFloat( int i ) {
    return row.getFloat( i );
  }

  @Override public double getDouble( int i ) {
    return row.getDouble( i );
  }

  @Override public String getString( int i ) {
    return row.getString( i );
  }

  @Override public BigDecimal getDecimal( int i ) {
    return row.getDecimal( i );
  }

  @Override public Date getDate( int i ) {
    return row.getDate( i );
  }

  @Override public Timestamp getTimestamp( int i ) {
    return row.getTimestamp( i );
  }

  @Override public <T> Seq<T> getSeq( int i ) {
    return row.getSeq( i );
  }

  @Override public <T> List<T> getList( int i ) {
    return row.getList( i );
  }

  @Override public <K, V> Map<K, V> getMap( int i ) {
    return row.getMap( i );
  }

  @Override public <K, V> java.util.Map<K, V> getJavaMap( int i ) {
    return row.getJavaMap( i );
  }

  @Override public org.apache.spark.sql.Row getStruct( int i ) {
    return row.getStruct( i );
  }

  @Override public <T> T getAs( int i ) {
    return row.getAs( i );
  }

  @Override public <T> T getAs( String fieldName ) {
    return row.getAs( fieldName );
  }

  @Override public int fieldIndex( String name ) {
    return row.fieldIndex( name );
  }

  @Override public <T> scala.collection.immutable.Map<String, T> getValuesMap( Seq<String> fieldNames ) {
    return row.getValuesMap( fieldNames );
  }

  @Override public String toString() {
    return row.toString();
  }

  @Override public org.apache.spark.sql.Row copy() {
    return row.copy();
  }

  @Override public boolean anyNull() {
    return row.anyNull();
  }
}
