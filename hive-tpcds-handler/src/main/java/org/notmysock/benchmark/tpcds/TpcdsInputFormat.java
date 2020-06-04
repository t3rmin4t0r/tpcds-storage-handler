package org.notmysock.benchmark.tpcds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport.Support;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.teradata.tpcds.Table;

public class TpcdsInputFormat implements
    org.apache.hadoop.mapred.InputFormat<NullWritable, TpcdsRow> /*,
    VectorizedInputFormatInterface*/ {

  /*
  @Override
  public Support[] getSupportedFeatures() {
    // TODO: decimal64
    return new VectorizedSupport.Support[0];
  }*/

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
    return computeSplits(jobConf);
  }

  private InputSplit[] computeSplits(Configuration conf) {
    String table = conf.get(TpcdsTableProperties.TABLE_NAME.getKey());
    int scale = Integer.parseInt(conf.get(TpcdsTableProperties.SCALE.getKey()));
    int parallel = Integer
        .parseInt(conf.get(TpcdsTableProperties.PARALLEL.getKey(), ""+scale));

    Table t = Table.getTable(table);
    if (t.isSmall()) {
      return new InputSplit[] { new TpcdsSplit(table, scale, parallel, 0) };
    }
    InputSplit[] splits = new InputSplit[parallel];
    for (int i = 0; i < parallel; i++) {
      splits[i] = new TpcdsSplit(table, scale, parallel, i);
    }
    return splits;
  }

  @Override
  public RecordReader<NullWritable, TpcdsRow> getRecordReader(
      InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
    /*
    if (Utilities.getIsVectorized(jobConf)) {
      // noinspection unchecked
      return (RecordReader) new VectorizedTpcdsReader((TpcdsSplit) split,
          jobConf);
    }*/
    return new TpcdsRowReader((TpcdsSplit) split, jobConf);
  }
}
