package org.notmysock.benchmark.tpcds.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import org.notmysock.benchmark.tpcds.TextTpcdsSerDe;
import org.notmysock.benchmark.tpcds.TpcdsInputFormat;
import org.notmysock.benchmark.tpcds.TpcdsRow;
import org.notmysock.benchmark.tpcds.TpcdsTableProperties;

import com.teradata.tpcds.Table;
import com.teradata.tpcds.column.Column;
import com.teradata.tpcds.column.ColumnType;
import com.teradata.tpcds.column.ColumnType.Base;
import com.teradata.tpcds.generator.StoreSalesGeneratorColumn;

public class TestTextTpcdsSerDe {

  @Test
  public void testSerDeParsing() throws IOException, SerDeException {
    Integer scale = 1;
    JobConf jobConf = new JobConf();
    Table table = Table.INVENTORY;
    jobConf.set(TpcdsTableProperties.TABLE_NAME.getKey(), table.getName());
    jobConf.set(TpcdsTableProperties.SCALE.getKey(), scale.toString());
    TpcdsInputFormat inputformat = new TpcdsInputFormat();
    InputSplit[] splits = inputformat.getSplits(jobConf, 1);
    RecordReader<NullWritable, TpcdsRow> reader = inputformat.getRecordReader(splits[0], jobConf, null);
    Column[] columns = table.getColumns();
    
    StringBuilder columnNames = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();
    boolean first = true;
    for (Column col : columns) {
      if (first) {
        first = false;
      } else {
        columnNames.append(",");
        columnTypes.append(",");
      }
      columnNames.append(col.getName());
      columnTypes.append(columnTypeToHive(col.getType()));
    }
    
    Properties tblProperties = new Properties();
    
    tblProperties.setProperty(serdeConstants.LIST_COLUMNS, columnNames.toString());
    tblProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes.toString());
    
    TextTpcdsSerDe serde = new TextTpcdsSerDe();
    serde.initialize(jobConf, tblProperties);
    
    NullWritable key = reader.createKey();
    TpcdsRow value = reader.createValue();
    long count = 0;
    while(reader.next(key, value)) {
      Object deserialize = serde.deserialize(value);
      count++;
    }
    System.out.println("Got " + count);
  }
  
  private static String columnTypeToHive(ColumnType ct) {
    final String family;
    switch (ct.getBase()) {
    case IDENTIFIER:
      family = "bigint";
      break;
    case INTEGER:
      family = "int";
      break;
    case TIME:
      family = "timestamp";
      break;
    case CHAR:
    case DATE:
    case DECIMAL:
    case VARCHAR:
      family = ct.getBase().toString().toLowerCase();
      break;
    default:
      return "";
    }
    if (ct.getScale().isPresent()) {
      int scale = ct.getScale().get();
      if (ct.getPrecision().isPresent()) {
        int precision = ct.getPrecision().get();
        return String.format("%s(%d,%d)", family, precision, scale);
      }
      return String.format("%s(%d)", family, scale);
    }
    return family;
  }

}
