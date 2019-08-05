package com.mine.spark.performancetest.mappers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.mine.spark.performancetest.kettle.RowMetaInterface;
import com.mine.spark.performancetest.kettle.ValueMetaInterface;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.pentaho.di.engine.spark.api.SparkEngineException;
import org.pentaho.di.engine.spark.api.TypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.pentaho.di.engine.spark.api.Field;

import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_BIGNUMBER;
import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_DATE;
import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_INET;
import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_INTEGER;
import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_NUMBER;
import static com.mine.spark.performancetest.kettle.ValueMetaInterface.TYPE_TIMESTAMP;
import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;


/**
 * An implementation of TypeMapper which converts values to and from spark sql StructType values and supports retrieval
 * of spark sql schemas.
 */
public class StructTypeMapper implements TypeMapper<StructType> {

  static final long serialVersionUID = -687001492234005033L;

  private static final Map<Integer, DataType> kettleSparkTypeMap;
  private static final Map<Integer, DataType> readSparkTypeMap;
  static final DataType DEFAULT_DECIMAL_TYPE = DataTypes.createDecimalType( 18, 4 );
  protected List<Field> inputFields;
  protected List<Field> outputFields;

  private static final Logger LOG = LoggerFactory.getLogger( StructTypeMapper.class );

  private static final ImmutableMap<Integer, MapForSpark> KETTLE_TO_STRUCT_TYPE_CONVERSION =
      ImmutableMap.<Integer, MapForSpark>builder()
          .put( TYPE_INET, ( input ) -> ( (InetAddress) input ).getAddress() )
          .put( TYPE_DATE, ( input ) -> new java.sql.Date( ( (Date) input ).getTime() ) )
          .build();

  @FunctionalInterface interface MapForSpark extends Function<Object, Object>, Serializable {
  }


  static {
    kettleSparkTypeMap = new HashMap<>();
    kettleSparkTypeMap.put( TYPE_NUMBER, DataTypes.DoubleType );
    kettleSparkTypeMap.put( ValueMetaInterface.TYPE_STRING, StringType );
    kettleSparkTypeMap.put( ValueMetaInterface.TYPE_BOOLEAN, DataTypes.BooleanType );
    kettleSparkTypeMap.put( TYPE_INTEGER, DataTypes.LongType );
    kettleSparkTypeMap.put( TYPE_BIGNUMBER, DEFAULT_DECIMAL_TYPE );
    kettleSparkTypeMap.put( ValueMetaInterface.TYPE_BINARY, BinaryType );
    kettleSparkTypeMap.put( ValueMetaInterface.TYPE_SERIALIZABLE, BinaryType );
    // serializable?
    kettleSparkTypeMap.put( TYPE_INET, BinaryType );

    kettleSparkTypeMap.put( TYPE_DATE, DateType );
    kettleSparkTypeMap.put( TYPE_TIMESTAMP, TimestampType );


    // Used for reading in date and time outputFields as strings, since they may not match
    // the expected Spark SQL format.
    readSparkTypeMap = new HashMap<>( kettleSparkTypeMap );
    readSparkTypeMap.put( TYPE_DATE, StringType );
    readSparkTypeMap.put( TYPE_TIMESTAMP, StringType );
  }

  public StructTypeMapper() {
  }

  /**
   * Case where the input row metadata matches the output row metadata
   */
  public StructTypeMapper( RowMetaInterface outputMeta ) {
    inputFields = outputFields = from( outputMeta );
  }

  public StructTypeMapper( RowMetaInterface inputMeta, RowMetaInterface outputMeta ) {
    outputFields = from( outputMeta );
    inputFields = from( inputMeta );
  }

  public StructTypeMapper( List<Field> inputFields, List<Field> outputFields ) {
    this.outputFields = outputFields;
    this.inputFields = inputFields;
  }


  /**
   * maps an array of values TO types applicable to spark SQL E.g., InetAddress objects are mapped to a byte array
   **/
  @Override public Object[] mapValuesTo( Object[] vals ) {
    preconditions( vals );

    Object[] objects = new Object[ outputFields.size() ];
    for ( int i = 0; i < outputFields.size(); i++ ) {
      objects[ i ] = typeToSparkCompatible(
          vals[ inputIndex( i ) ],
          outputFields.get( i ) );
    }
    return objects;
  }

  private int inputIndex( int i ) {
    return inputFields.indexOf( correspondingInput( outputFields.get( i ).getName() ) );
  }

  protected Field correspondingInput( String outputFieldName ) {
    return inputFields.stream()
        .filter( f -> f.getName().equals( outputFieldName ) )
        .findFirst()
        .orElseThrow( () -> new SparkEngineException(
            msg( "ERROR.No_Matching_Input", outputFieldName ) ) );
  }


  /**
   * maps an array of values FROM types applicable to spark SQL E.g. a java.sql.Timestamp from Spark SQL may be mapped
   * to java.util.Date
   **/
  @Override public Object[] mapValuesFrom( StructType schema, Object[] vals ) {
    preconditions( vals );
    Preconditions.checkArgument( vals.length == schema.fields().length );

    StructField[] structFields = schema.fields();
    return IntStream.range( 0, structFields.length )
        .mapToObj(
            index -> typeToKettleCompatible( vals[ index ], structFields[ index ].dataType(),
                outputFields.get( index ) ) )
        .collect( Collectors.toList() ).toArray();
  }

  /**
   * Returns a StructType with schema corresponding to rowMeta
   **/
  @Override public StructType schema() {

    final StructType structType = getStructType( getSparkTypeMap() );
    LOG.debug( "Schema:  " + structType );
    return structType;
  }

  /**
   * Returns a StructType with schema corresponding to rowMeta. Differs from the above in that it treats Dates and
   * Timestamps as Strings when reading in to avoid formatting issues.
   **/
  @Override public StructType schemaForRead() {
    final StructType structType = getStructType( getSparkTypeMapForRead() );
    LOG.debug( "Schema:  " + structType );
    return structType;
  }


  private Map<Integer, DataType> getSparkTypeMap() {
    return kettleSparkTypeMap;
  }

  private Map<Integer, DataType> getSparkTypeMapForRead() {
    return readSparkTypeMap;
  }


  protected Object typeToKettleCompatible( Object value, DataType sparkSqlType, Field field ) {
    Object toRet = value;

    if ( value == null ) {
      return null;
    }
    if ( field.getType() == TYPE_DATE ) {
      toRet = handleDate( sparkSqlType, value, field );
    } else if ( field.getType() == TYPE_TIMESTAMP ) {
      toRet = handleTimestamp( sparkSqlType, value, field );
    } else if ( field.getType() == TYPE_INET ) {
      toRet = handleAddress( sparkSqlType, value, field );
    } else if ( field.getType() == TYPE_INTEGER ) {
      toRet = handleIntegers( sparkSqlType, value, field );
    } else if ( field.getType() == TYPE_NUMBER ) {
      Class clsType = value.getClass();
      // If it is TYPE_NUMBER it must be upcasted or converted to double.
      // The reason for this is getNumber in ValueMetaBase castes all TYPE_NUMBER to double.
      // https://github.com/pentaho/pentaho-kettle/blob/master/core/src/main/java/org/pentaho/di/core/row/value
      // /ValueMetaBase.java#L1960
      if ( clsType != java.lang.Double.class ) {
        if ( clsType == java.lang.Long.class ) {
          toRet = ( (Long) value ).doubleValue();
        } else {
          // Convert to double
          toRet = new Double( value.toString() );
        }
      }
    } else if ( field.getType() == TYPE_BIGNUMBER ) {
      // If the class is not BigDecimal all downstream fields expect this.
      if ( value.getClass() != java.math.BigDecimal.class ) {
        toRet = new BigDecimal( value.toString() );
      }
    }

    return toRet;
  }


  private Object typeToSparkCompatible( Object object, Field field ) {
    Object result = object;
    if ( result == null && field.isNullable() ) {
      return null;
    }
    return Optional.ofNullable( KETTLE_TO_STRUCT_TYPE_CONVERSION.get( field.getType() ) )
        .orElse( ( o ) -> o )
        .apply( result );
  }

//  private Object nullValToDefault( Field field ) {
//    try {
//      ValueMetaInterface valueMetaBase =
//          ValueMetaFactory.createValueMeta(
//              field.getName(), field.getType(), field.getLength(), field.getPrecision() );
//
//      ValueMetaString stringValueMeta = new ValueMetaString();
//      if ( field.getType() == TYPE_DATE || field.getType() == TYPE_TIMESTAMP ) {
//        valueMetaBase.setConversionMask( field.getDateFormat().toPattern() );
//        stringValueMeta.setConversionMask( field.getDateFormat().toPattern() );
//      }
//      return valueMetaBase.convertDataFromString(
//          field.getIfNull().toString(), stringValueMeta, null, null,
//          ValueMetaInterface.TRIM_TYPE_BOTH );
//    } catch ( KettlePluginException | KettleValueException e ) {
//      throw new SparkEngineException( e );
//    }
//
//  }

  private void preconditions( Object[] vals ) {
    Preconditions.checkNotNull( outputFields );
    Preconditions.checkNotNull( vals );
    // kettle can allocate larger arrays than are needed, so can't check for ==
    Preconditions.checkArgument( outputFields.size() <= vals.length );
  }

  private Date handleDate( DataType sparkSqlType, Object value, Field field ) {
    // Kettle's Date uses java.util.Date, which allows for time down to millis.
    // Because of this, TimestampType is expected here (not DateType),
    // otherwise time truncation could occur.  StringType is also allowed, so
    // arbitrary formats can be parsed.

    if ( sparkSqlType == DateType ) {
      return (Date) value;
    } else if ( sparkSqlType == StringType ) {
      return parseDate( (String) value, field );
    }
    throw new RuntimeException( "Died" );
  }

  private Timestamp handleTimestamp( DataType sparkSqlType, Object value, Field field ) {
    // Kettle's Date uses java.util.Date, which allows for time down to millis.
    // Because of this, TimestampType is expected here (not DateType),
    // otherwise time truncation could occur.  StringType is also allowed, so
    // arbitrary formats can be parsed.

    if ( sparkSqlType == TimestampType ) {
      return (Timestamp) value;
    } else if ( sparkSqlType == StringType ) {
      return parseTimestamp( (String) value, field );
    }
    throw new RuntimeException( "Died" );
  }

  private InetAddress handleAddress( DataType sparkSqlType, Object value, Field field ) {
    try {
      if ( sparkSqlType == BinaryType ) {
        return value == null ? null : InetAddress.getByAddress( (byte[]) value );
      }
    } catch ( UnknownHostException e ) {
      throw new RuntimeException( e.getMessage() );
    }
    throw new RuntimeException( "Died" );
  }

  private Date parseDate( String value, Field field ) {
    try {
      Date date = field.getDateFormat().parse( value );
      return new java.sql.Date( date.getTime() );
    } catch ( ParseException e ) {
      throw new RuntimeException( e );
    }
  }

  private Timestamp parseTimestamp( String value, Field field ) {
    try {
      Date date = field.getDateFormat().parse( value );
      return new Timestamp( date.getTime() );
    } catch ( ParseException e ) {
      throw new RuntimeException( e );
    }
  }


  private StructType getStructType( Map<Integer, DataType> kettleSparkTypeMap ) {
    List<StructField> structFields =
        outputFields.stream()
            .map( field -> DataTypes.createStructField(
                field.getPath(), getSparkType( field, kettleSparkTypeMap ), field.isNullable() ) )
            .collect( Collectors.toList() );

    return DataTypes.createStructType( structFields );
  }

  private static DataType getSparkType( Field field, Map<Integer, DataType> mappingTypes ) {
    DataType sparkDataType = mappingTypes.get( field.getType() );
    Preconditions.checkNotNull( sparkDataType,
        msg( "ERROR.Spark_Datatype_Not_Found", field.getName(), field.getType() ) );

    if ( sparkDataType.sameType( DEFAULT_DECIMAL_TYPE ) ) {
      int length = field.getLength();
      int precision = field.getPrecision();

      //otherwise just use the default type
      if ( length > 0 ) {
        precision = ( precision > length ) ? length : precision;
        return DataTypes.createDecimalType( length, ( precision < 0 ) ? 0 : precision );
      }
    }
    return sparkDataType;
  }

  public static Field fromField( int fieldIndex, RowMetaInterface rowMeta ) {
    final ValueMetaInterface valueMeta = rowMeta.getValueMeta( fieldIndex );
    final int type = valueMeta.getType();
    Field.FieldBuilder builder = Field.builder()
        .name( rowMeta.getFieldNames()[ fieldIndex ] )
        .type( type )
        .precision( valueMeta.getPrecision() )
        .length( valueMeta.getLength() )
        .dateFormat( valueMeta.getDateFormat() );
    if ( type == TYPE_DATE || type == TYPE_TIMESTAMP ) {
      Preconditions.checkArgument( valueMeta.getFormatMask() != null );
      builder.dateFormat( valueMeta.getDateFormat() );
    }

    return builder.build();
  }

  public static Field fromField( String fieldName, RowMetaInterface rowMeta ) {
    int fieldIndex = -1;
    String fowMetaFieldName = null;

    for ( int i = 0; i < rowMeta.size(); i++ ) {
      fowMetaFieldName = rowMeta.getFieldNames()[ i ];

      if ( fowMetaFieldName.equals( fieldName ) ) {
        fieldIndex = i;
      }
    }

    if ( fieldIndex < 0 ) {
      return null;
    }

    return fromField( fieldIndex, rowMeta );
  }

  public static List<Field> from( RowMetaInterface rowMeta ) {
    final ArrayList<Field> fields = new ArrayList<>();
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      Field field = fromField( i, rowMeta );

      fields.add( field );
    }
    return fields;
  }

  protected static String msg( String key, Object... args ) {
    return "Died";//BaseMessages.getString( TypeMapper.class, key, args );
  }

  // This method will return long for none BigDecimal values. This is to avoid a class cast
  // exception in getString in ValueMetaBase:
  // https://github.com/pentaho/pentaho-kettle/blob/master/core/src/main/java/org/pentaho/di/core/row/value
  // /ValueMetaBase.java#L1834
  // Kettle expects all ints to be up cast to long.
  private Object handleIntegers( DataType sparkSqlType, Object value, Field field ) {
    Class sparkSqlClassType = value.getClass();
    if ( sparkSqlClassType == java.lang.Long.class ) {
      return value;
    } else if ( sparkSqlClassType == java.lang.Integer.class ) {
      // INT
      return ( (Integer) value ).longValue();
    } else if ( sparkSqlClassType == java.lang.Short.class ) {
      // SMALLINT
      return ( (Short) value ).longValue();
    } else if ( sparkSqlClassType == java.lang.Byte.class ) {
      // TINYINT
      return ( (Byte) value ).longValue();
    } else if ( sparkSqlClassType == java.math.BigDecimal.class ) {
      // BIGINT
      // Certain JDBC drivers support isSigned and some do not. The ones that do not cast the bigint to BigDecimal so
      // not to lose information. THIS VARIES BY VENDOR!!!!
      // See https://github.com/pentaho/pentaho-kettle/blob/master/core/src/main/java/org/pentaho/di/core/row/value
      // /ValueMetaBase.java#L4718
      // Set scale to zero to not have decimal places and HALF_UP is used based on Kettle.
      // Also Kettle expects a BigDecimal returned when the significance is greater than 18. For instance
      // decimal( 18, 0 ) returns long and decimal( 19, 0 ) returns BigDecimal
      try {
        return ( (BigDecimal) value ).longValueExact();
      } catch ( ArithmeticException ae ) {
        return ( (BigDecimal) value ).setScale( 0, RoundingMode.HALF_UP );
      }
    } else if ( sparkSqlClassType == java.lang.Double.class ) {
      return ( (Double) value ).longValue();
    }

    throw new RuntimeException( "Died" );
  }


}
