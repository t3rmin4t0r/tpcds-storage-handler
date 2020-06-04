package org.notmysock.benchmark.tpcds;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextTpcdsSerDe extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory
      .getLogger(TextTpcdsSerDe.class);
  private ObjectInspector inspector;

  final long rawDataSize = 0;
  final long rowCount = 0;
  private List<String> columnNames;
  private ArrayList<TypeInfo> columnTypes;

  static Function<TypeInfo, ObjectInspector> typeInfoToObjectInspector = typeInfo -> PrimitiveObjectInspectorFactory
      .getPrimitiveWritableObjectInspector(
          TypeInfoFactory.getPrimitiveTypeInfo(typeInfo.getTypeName()));

  private static final ThreadLocal<DateTimeFormatter> TS_PARSER = ThreadLocal
      .withInitial(TextTpcdsSerDe::createAutoParser);

  private static final Function<TypeInfo, ObjectInspector> TYPEINFO_TO_OI = typeInfo -> PrimitiveObjectInspectorFactory
      .getPrimitiveWritableObjectInspector(
          TypeInfoFactory.getPrimitiveTypeInfo(typeInfo.getTypeName()));
  
  private static final Function<TypeInfo, PrimitiveTypeInfo> TYPEINFO_TO_PTI = typeInfo -> TypeInfoFactory
      .getPrimitiveTypeInfo(typeInfo.getTypeName());

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    final List<ObjectInspector> inspectors;
    // Get column names and types
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl
        .getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl
        .containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
            ? tbl.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
            : String.valueOf(SerDeUtils.COMMA);
    // all table column names
    if (!columnNameProperty.isEmpty()) {
      columnNames = Arrays
          .asList(columnNameProperty.split(columnNameDelimiter));
    }
    // all column types
    if (!columnTypeProperty.isEmpty()) {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("columns: {}, {}", columnNameProperty, columnNames);
      LOG.debug("types: {}, {} ", columnTypeProperty, columnTypes);
    }

    inspectors = columnTypes.stream().map(TYPEINFO_TO_OI)
        .collect(Collectors.toList());
    inspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames, inspectors);
  }

  @Override
  public Object deserialize(Writable rawRow) throws SerDeException {
    TpcdsRow row = (TpcdsRow) rawRow;
    // takes a tpc-ds row and need to return a Standard struct back
    // this needn't be a copy?
    final List<Object> output = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      output.add(parse(row.getRaw(i), columnTypes.get(i)));
    }
    return output;
  }

  private static Object parse(String value, TypeInfo typeInfo) throws SerDeException {
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
    if (value == null) {
      return value;
    }
    if (value.isEmpty()) {
      if (!(PrimitiveObjectInspectorUtils
          .getPrimitiveGrouping(pti.getPrimitiveCategory()) == STRING_GROUP)) {
        // Strings can be blank
        return null;
      }
    }
    try {
      switch (pti.getPrimitiveCategory()) {
      case TIMESTAMP:
        TimestampWritable timestampWritable = new TimestampWritable();
        timestampWritable.setTime(TS_PARSER.get().parseMillis(value));
        return timestampWritable;
      case TIMESTAMPLOCALTZ:
        final long numberOfMillis = TS_PARSER.get().parseMillis(value);
        return new TimestampLocalTZWritable(new TimestampTZ(
            ZonedDateTime.ofInstant(Instant.ofEpochMilli(numberOfMillis),
                ((TimestampLocalTZTypeInfo) pti).timeZone())));
      case BYTE:
        return new ByteWritable(Byte.parseByte(value));
      case SHORT:
        return (new ShortWritable(Short.parseShort(value)));
      case INT:
        return new IntWritable(Integer.parseInt(value));
      case LONG:
        return (new LongWritable(Long.parseLong(value)));
      case FLOAT:
        return (new FloatWritable(Float.parseFloat(value)));
      case DOUBLE:
        return (new DoubleWritable(Double.parseDouble(value)));
      case DECIMAL:
        return (new HiveDecimalWritable(HiveDecimal.create(value)));
      case CHAR:
        return (new HiveCharWritable(
            new HiveChar(value, ((CharTypeInfo) pti).getLength())));
      case VARCHAR:
        return (new HiveVarcharWritable(
            new HiveVarchar(value, ((CharTypeInfo) pti).getLength())));
      case STRING:
        return (new Text(value));
      case BOOLEAN:
        return (new BooleanWritable(Boolean.valueOf(value)));
      default:
        throw new SerDeException("Unknown type: " + pti.getTypeName());
      }
    } catch (NumberFormatException nfe) {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public SerDeStats getSerDeStats() {
    SerDeStats serDeStats = new SerDeStats();
    serDeStats.setRawDataSize(rawDataSize);
    serDeStats.setRowCount(rowCount);
    return serDeStats;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return TpcdsRow.class;
  }

  @Override
  public Writable serialize(Object arg0, ObjectInspector arg1)
      throws SerDeException {
    return null;
  }

  private static DateTimeFormatter createAutoParser() {
    final DateTimeFormatter offsetElement = new DateTimeFormatterBuilder()
        .appendTimeZoneOffset("Z", true, 2, 4).toFormatter();

    DateTimeParser timeOrOffset = new DateTimeFormatterBuilder()
        .append(null,
            new DateTimeParser[] {
                new DateTimeFormatterBuilder().appendLiteral('T').toParser(),
                new DateTimeFormatterBuilder().appendLiteral(' ').toParser() })
        .appendOptional(ISODateTimeFormat.timeElementParser().getParser())
        .appendOptional(offsetElement.getParser()).toParser();

    return new DateTimeFormatterBuilder()
        .append(ISODateTimeFormat.dateElementParser())
        .appendOptional(timeOrOffset).toFormatter();
  }

}
