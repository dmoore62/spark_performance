package com.mine.spark.performancetest.kettle;

import org.w3c.dom.Node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class ValueMetaString implements ValueMetaInterface {
  private String name;

  public ValueMetaString( String name ) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setName( String name ) {

  }

  @Override
  public int getLength() {
    return 0;
  }

  @Override
  public void setLength( int length ) {

  }

  @Override
  public int getPrecision() {
    return 0;
  }

  @Override
  public void setPrecision( int precision ) {

  }

  @Override
  public void setLength( int length, int precision ) {

  }

  @Override
  public String getOrigin() {
    return null;
  }

  @Override
  public void setOrigin( String origin ) {

  }

  @Override
  public String getComments() {
    return null;
  }

  @Override
  public void setComments( String comments ) {

  }

  @Override
  public int getType() {
    return 0;
  }

  @Override
  public void setType( int type ) {

  }

  @Override
  public int getStorageType() {
    return 0;
  }

  @Override
  public void setStorageType( int storageType ) {

  }

  @Override
  public int getTrimType() {
    return 0;
  }

  @Override
  public void setTrimType( int trimType ) {

  }

  @Override
  public Object[] getIndex() {
    return new Object[0];
  }

  @Override
  public void setIndex( Object[] index ) {

  }

  @Override
  public boolean isStorageNormal() {
    return false;
  }

  @Override
  public boolean isStorageIndexed() {
    return false;
  }

  @Override
  public boolean isStorageBinaryString() {
    return false;
  }

  @Override
  public String getConversionMask() {
    return null;
  }

  @Override
  public void setConversionMask( String conversionMask ) {

  }

  @Override
  public String getFormatMask() {
    return null;
  }

  @Override
  public String getDecimalSymbol() {
    return null;
  }

  @Override
  public void setDecimalSymbol( String decimalSymbol ) {

  }

  @Override
  public String getGroupingSymbol() {
    return null;
  }

  @Override
  public void setGroupingSymbol( String groupingSymbol ) {

  }

  @Override
  public String getCurrencySymbol() {
    return null;
  }

  @Override
  public void setCurrencySymbol( String currencySymbol ) {

  }

  @Override
  public SimpleDateFormat getDateFormat() {
    return null;
  }

  @Override
  public DecimalFormat getDecimalFormat() {
    return null;
  }

  @Override
  public DecimalFormat getDecimalFormat( boolean useBigDecimal ) {
    return null;
  }

  @Override
  public String getStringEncoding() {
    return null;
  }

  @Override
  public void setStringEncoding( String stringEncoding ) {

  }

  @Override
  public boolean isSingleByteEncoding() {
    return false;
  }

  @Override
  public boolean isNull( Object data ) throws KettleValueException {
    return false;
  }

  @Override
  public boolean isCaseInsensitive() {
    return false;
  }

  @Override
  public void setCaseInsensitive( boolean caseInsensitive ) {

  }

  @Override
  public boolean isCollatorDisabled() {
    return false;
  }

  @Override
  public void setCollatorDisabled( boolean collatorDisabled ) {

  }

  @Override
  public Locale getCollatorLocale() {
    return null;
  }

  @Override
  public void setCollatorLocale( Locale locale ) {

  }

  @Override
  public int getCollatorStrength() {
    return 0;
  }

  @Override
  public void setCollatorStrength( int collatorStrength ) throws IllegalArgumentException {

  }

  @Override
  public boolean isSortedDescending() {
    return false;
  }

  @Override
  public void setSortedDescending( boolean sortedDescending ) {

  }

  @Override
  public boolean isOutputPaddingEnabled() {
    return false;
  }

  @Override
  public void setOutputPaddingEnabled( boolean outputPaddingEnabled ) {

  }

  @Override
  public boolean isLargeTextField() {
    return false;
  }

  @Override
  public void setLargeTextField( boolean largeTextField ) {

  }

  @Override
  public boolean isDateFormatLenient() {
    return false;
  }

  @Override
  public void setDateFormatLenient( boolean dateFormatLenient ) {

  }

  @Override
  public Locale getDateFormatLocale() {
    return null;
  }

  @Override
  public void setDateFormatLocale( Locale dateFormatLocale ) {

  }

  @Override
  public TimeZone getDateFormatTimeZone() {
    return null;
  }

  @Override
  public void setDateFormatTimeZone( TimeZone dateFormatTimeZone ) {

  }

  @Override
  public int getOriginalColumnType() {
    return 0;
  }

  @Override
  public void setOriginalColumnType( int originalColumnType ) {

  }

  @Override
  public String getOriginalColumnTypeName() {
    return null;
  }

  @Override
  public void setOriginalColumnTypeName( String originalColumnTypeName ) {

  }

  @Override
  public int getOriginalPrecision() {
    return 0;
  }

  @Override
  public void setOriginalPrecision( int originalPrecision ) {

  }

  @Override
  public int getOriginalScale() {
    return 0;
  }

  @Override
  public int getOriginalNullable() {
    return 0;
  }

  @Override
  public boolean getOriginalSigned() {
    return false;
  }

  @Override
  public void setOriginalScale( int originalScale ) {

  }

  @Override
  public boolean isOriginalAutoIncrement() {
    return false;
  }

  @Override
  public void setOriginalAutoIncrement( boolean originalAutoIncrement ) {

  }

  @Override
  public int isOriginalNullable() {
    return 0;
  }

  @Override
  public void setOriginalNullable( int originalNullable ) {

  }

  @Override
  public boolean isOriginalSigned() {
    return false;
  }

  @Override
  public void setOriginalSigned( boolean originalSigned ) {

  }

  @Override
  public Object cloneValueData( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public String getCompatibleString( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public String getString( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public byte[] getBinaryString( Object object ) throws KettleValueException {
    return new byte[0];
  }

  @Override
  public Double getNumber( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public BigDecimal getBigNumber( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Long getInteger( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Date getDate( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Boolean getBoolean( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public byte[] getBinary( Object object ) throws KettleValueException {
    return new byte[0];
  }

  @Override
  public ValueMetaInterface clone() {
    return null;
  }

  @Override
  public boolean isString() {
    return false;
  }

  @Override
  public boolean isDate() {
    return false;
  }

  @Override
  public boolean isBigNumber() {
    return false;
  }

  @Override
  public boolean isNumber() {
    return false;
  }

  @Override
  public boolean isBoolean() {
    return false;
  }

  @Override
  public boolean isSerializableType() {
    return false;
  }

  @Override
  public boolean isBinary() {
    return false;
  }

  @Override
  public boolean isInteger() {
    return false;
  }

  @Override
  public boolean isNumeric() {
    return false;
  }

  @Override
  public String getTypeDesc() {
    return null;
  }

  @Override
  public String toStringMeta() {
    return null;
  }

  @Override
  public void writeMeta( DataOutputStream outputStream ) throws KettleFileException {

  }

  @Override
  public void writeData( DataOutputStream outputStream, Object object ) throws KettleFileException {

  }

  @Override
  public Object readData( DataInputStream inputStream ) throws KettleFileException, SocketTimeoutException {
    return null;
  }

  @Override
  public void readMetaData( DataInputStream inputStream ) throws KettleFileException {

  }

  @Override
  public int compare( Object data1, Object data2 ) throws KettleValueException {
    return 0;
  }

  @Override
  public int compare( Object data1, ValueMetaInterface meta2, Object data2 ) throws KettleValueException {
    return 0;
  }

  @Override
  public Object convertData( ValueMetaInterface meta2, Object data2 ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertDataCompatible( ValueMetaInterface meta2, Object data2 ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertDataUsingConversionMetaData( Object data ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertDataFromString( String pol, ValueMetaInterface convertMeta, String nullif, String ifNull, int trim_type ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertToNormalStorageType( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertBinaryStringToNativeType( byte[] binary ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertNormalStorageTypeToBinaryString( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Object convertToBinaryStringStorageType( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public int hashCode( Object object ) throws KettleValueException {
    return 0;
  }

  @Override
  public ValueMetaInterface getStorageMetadata() {
    return null;
  }

  @Override
  public void setStorageMetadata( ValueMetaInterface storageMetadata ) {

  }

  @Override
  public ValueMetaInterface getConversionMetadata() {
    return null;
  }

  @Override
  public void setConversionMetadata( ValueMetaInterface conversionMetadata ) {

  }

  @Override
  public String getMetaXML() throws IOException {
    return null;
  }

  @Override
  public String getDataXML( Object value ) throws IOException {
    return null;
  }

  @Override
  public Object getValue( Node node ) throws KettleException {
    return null;
  }

  @Override
  public long getNumberOfBinaryStringConversions() {
    return 0;
  }

  @Override
  public void setNumberOfBinaryStringConversions( long numberOfBinaryStringConversions ) {

  }

  @Override
  public boolean requiresRealClone() {
    return false;
  }

  @Override
  public boolean isLenientStringToNumber() {
    return false;
  }

  @Override
  public void setLenientStringToNumber( boolean lenientStringToNumber ) {

  }

  @Override
  public Object getNativeDataType( Object object ) throws KettleValueException {
    return null;
  }

  @Override
  public Class<?> getNativeDataTypeClass() throws KettleValueException {
    return null;
  }

  @Override
  public boolean isIgnoreWhitespace() {
    return false;
  }

  @Override
  public void setIgnoreWhitespace( boolean ignoreWhitespace ) {

  }
}
