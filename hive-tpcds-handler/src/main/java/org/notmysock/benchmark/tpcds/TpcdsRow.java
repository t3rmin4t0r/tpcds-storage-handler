package org.notmysock.benchmark.tpcds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.Writable;

import com.teradata.tpcds.Table;
import com.teradata.tpcds.row.TableRow;

public class TpcdsRow implements Writable {

  private Table table;
  private StringBuilder sb = new StringBuilder();
  private List<String> values = Collections.EMPTY_LIST;
  
  public TpcdsRow(Table table) {
    this.table = table;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }
  

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException("read unsupported");
  }

  public String getRaw(int i) {
    return values.get(i);
  }

  public void assign(TableRow tableRow) {
    this.values  = tableRow.getValues();
  }
  
  @Override
  public String toString() {
    // this is called premature optimization
    StringBuilder sb = new StringBuilder(values.size()*8+16);
    boolean first = true;
    for (String col : values) {
      if (first) {
        first = false;
      } else {
        sb.append("|");
      }
      sb.append(col);
    }
    return sb.toString();
  }

}
