package org.notmysock.benchmark.tpcds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;

public class VectorizedTpcdsReader
    implements RecordReader<NullWritable, VectorizedRowBatch> {

  public VectorizedTpcdsReader(TpcdsSplit split, Configuration jobConf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean next(NullWritable key, VectorizedRowBatch value)
      throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public NullWritable createKey() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public VectorizedRowBatch createValue() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
}
