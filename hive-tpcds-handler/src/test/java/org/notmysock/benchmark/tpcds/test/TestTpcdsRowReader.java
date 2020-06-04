package org.notmysock.benchmark.tpcds.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;
import org.notmysock.benchmark.tpcds.TpcdsRow;
import org.notmysock.benchmark.tpcds.TpcdsRowReader;
import org.notmysock.benchmark.tpcds.TpcdsSplit;

public class TestTpcdsRowReader {

  @Test
  public void testDateDimReader() throws IOException {
    Configuration conf = new  Configuration();
    TpcdsSplit split = new TpcdsSplit("date_dim", 1, 1, 1);
    try (TpcdsRowReader reader = new TpcdsRowReader(split, conf)) {
      NullWritable key = reader.createKey();
      TpcdsRow value = reader.createValue();
      while(reader.next(key, value)) {
        assertNotNull(value.toString());
      }
    }
  }
  
  @Test
  public void testStoreReturnsReader() throws IOException {
    Configuration conf = new  Configuration();
    TpcdsSplit split = new TpcdsSplit("store_returns", 1, 1, 1);
    try (TpcdsRowReader reader = new TpcdsRowReader(split, conf)) {
      NullWritable key = reader.createKey();
      TpcdsRow value = reader.createValue();
      while(reader.next(key, value)) {
        assertNotNull(value.toString());
      }
    }
  }

}
