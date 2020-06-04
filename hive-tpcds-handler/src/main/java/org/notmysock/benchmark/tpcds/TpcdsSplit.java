package org.notmysock.benchmark.tpcds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;

public final class TpcdsSplit extends FileSplit
    implements org.apache.hadoop.mapred.InputSplit {

  private static final String[] EMPTY_ARRAY = new String[0];
  private String table;
  private int scale;
  private int parallel;
  private int child;
  private Path dummyPath;

  public TpcdsSplit() {
  }

  public TpcdsSplit(String table, int scale, int parallel, int child, Path dummyPath) {
    this.table = table;
    this.scale = scale;
    this.parallel = parallel;
    this.child = child;
    this.dummyPath = dummyPath;
  }

  public int getScale() {
    return scale;
  }

  public String getTable() {
    return table;
  }

  public int getParallel() {
    return parallel;
  }

  public int getChild() {
    return child;
  }
  
  @Override
  public Path getPath() {
    // this stupid, but we're using FileSplit for everything
    return dummyPath;
  }

  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return EMPTY_ARRAY;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.table = input.readUTF();
    this.scale = input.readInt();
    this.parallel = input.readInt();
    this.child = input.readInt();
    this.dummyPath = new Path(input.readUTF());
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeUTF(table);
    output.writeInt(scale);
    output.writeInt(parallel);
    output.writeInt(child);
    output.writeUTF(dummyPath.toString());
  }
  
  public Session getSession() {
    return Session.getDefaultSession().withScale(scale)
        .withTable(Table.getTable(table)).withParallelism(parallel)
        .withChunkNumber(child+1);
  }

}
