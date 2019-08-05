package com.mine.spark.performancetest.kettle;

import org.w3c.dom.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class RowMeta implements RowMetaInterface {
  List<ValueMetaInterface> valueMetaList;

  public RowMeta() {
    this.valueMetaList = new ArrayList<>();
  }

  public RowMeta( List<ValueMetaInterface> valueMetaList ) {
    this.valueMetaList = valueMetaList;
  }

  @Override
  public List<ValueMetaInterface> getValueMetaList() {
    return valueMetaList;
  }

  @Override
  public void setValueMetaList( List<ValueMetaInterface> valueMetaList ) {
    this.valueMetaList = valueMetaList;
  }

  @Override
  public boolean exists( ValueMetaInterface meta ) {
    return false;
  }

  @Override
  public void addValueMeta( ValueMetaInterface meta ) {

  }

  @Override
  public void addValueMeta( int index, ValueMetaInterface meta ) {
    valueMetaList.add( index, meta );
  }

  @Override
  public ValueMetaInterface getValueMeta( int index ) {
    return valueMetaList.get( index );
  }

  @Override
  public void setValueMeta( int index, ValueMetaInterface valueMeta ) {

  }

  @Override
  public String getString( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public Long getInteger( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public Double getNumber( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public Date getDate( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public BigDecimal getBigNumber( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public Boolean getBoolean( Object[] dataRow, int index ) throws KettleValueException {
    return null;
  }

  @Override
  public byte[] getBinary( Object[] dataRow, int index ) throws KettleValueException {
    return new byte[0];
  }

  @Override
  public Object[] cloneRow( Object[] objects, Object[] cloneTo ) throws KettleValueException {
    return new Object[0];
  }

  @Override
  public Object[] cloneRow( Object[] objects ) throws KettleValueException {
    return new Object[0];
  }

  @Override
  public int size() {
    return valueMetaList.size();
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean isNull( Object[] dataRow, int index ) throws KettleValueException {
    return false;
  }

  @Override
  public RowMetaInterface clone() {
    return null;
  }

  @Override
  public RowMetaInterface cloneToType( int targetType ) throws KettleValueException {
    return null;
  }

  @Override
  public String getString( Object[] dataRow, String valueName, String defaultValue ) throws KettleValueException {
    return null;
  }

  @Override
  public Long getInteger( Object[] dataRow, String valueName, Long defaultValue ) throws KettleValueException {
    return null;
  }

  @Override
  public Date getDate( Object[] dataRow, String valueName, Date defaultValue ) throws KettleValueException {
    return null;
  }

  @Override
  public ValueMetaInterface searchValueMeta( String valueName ) {
    return null;
  }

  @Override
  public int indexOfValue( String valueName ) {
    return 0;
  }

  @Override
  public void addRowMeta( RowMetaInterface rowMeta ) {

  }

  @Override
  public void mergeRowMeta( RowMetaInterface r ) {

  }

  @Override
  public void mergeRowMeta( RowMetaInterface r, String originStepName ) {

  }

  @Override
  public String[] getFieldNames() {
    String[] retval = new String[ size() ];

    for ( int i = 0; i < size(); i++ ) {
      String valueName = getValueMeta( i ).getName();
      retval[i] = valueName == null ? "" : valueName;
    }

    return retval;
  }

  @Override
  public void writeMeta( DataOutputStream outputStream ) throws KettleFileException {

  }

  @Override
  public void writeData( DataOutputStream outputStream, Object[] data ) throws KettleFileException {

  }

  @Override
  public Object[] readData( DataInputStream inputStream ) throws KettleFileException, SocketTimeoutException {
    return new Object[0];
  }

  @Override
  public void clear() {

  }

  @Override
  public void removeValueMeta( String string ) throws KettleValueException {

  }

  @Override
  public void removeValueMeta( int index ) {

  }

  @Override
  public String getString( Object[] row ) throws KettleValueException {
    return null;
  }

  @Override
  public String[] getFieldNamesAndTypes( int maxlen ) {
    return new String[0];
  }

  @Override
  public int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws KettleValueException {
    return 0;
  }

  @Override
  public boolean equals( Object[] rowData1, Object[] rowData2, int[] fieldnrs ) throws KettleValueException {
    return false;
  }

  @Override
  public int compare( Object[] rowData1, Object[] rowData2, int[] fieldnrs1, int[] fieldnrs2 ) throws KettleValueException {
    return 0;
  }

  @Override
  public int compare( Object[] rowData1, RowMetaInterface rowMeta2, Object[] rowData2, int[] fieldnrs1, int[] fieldnrs2 ) throws KettleValueException {
    return 0;
  }

  @Override
  public int compare( Object[] rowData1, Object[] rowData2 ) throws KettleValueException {
    return 0;
  }

  @Override
  public int oldXORHashCode( Object[] rowData ) throws KettleValueException {
    return 0;
  }

  @Override
  public int hashCode( Object[] rowData ) throws KettleValueException {
    return 0;
  }

  @Override
  public int convertedValuesHashCode( Object[] rowData ) throws KettleValueException {
    return 0;
  }

  @Override
  public String toStringMeta() {
    return null;
  }

  @Override
  public String getMetaXML() throws IOException {
    return null;
  }

  @Override
  public String getDataXML( Object[] rowData ) throws IOException {
    return null;
  }

  @Override
  public Object[] getRow( Node node ) throws KettleException {
    return new Object[0];
  }
}
