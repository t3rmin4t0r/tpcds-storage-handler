package org.notmysock.benchmark.tpcds.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.notmysock.benchmark.tpcds.TpcdsInputFormat;
import org.notmysock.benchmark.tpcds.TpcdsTableProperties;

public class TpcdsInputFormatTest {

  @Test
  public void testGetSplits() throws IOException {
    Integer scale = 100;
    JobConf jobConf = new JobConf();
    jobConf.set(TpcdsTableProperties.TABLE_NAME.getKey(), "store_sales");
    jobConf.set(TpcdsTableProperties.SCALE.getKey(), scale.toString());
    TpcdsInputFormat inputformat = new TpcdsInputFormat();
    InputSplit[] splits = inputformat.getSplits(jobConf, 1);
    assertEquals((int)scale, splits.length);
  }

}
