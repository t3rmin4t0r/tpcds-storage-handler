package org.notmysock.benchmark.tpcds;

import static com.teradata.tpcds.Parallel.splitWork;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import com.teradata.tpcds.Parallel.ChunkBoundaries;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;
import com.teradata.tpcds.TpcdsException;
import com.teradata.tpcds.row.TableRow;
import com.teradata.tpcds.row.generator.RowGenerator;
import com.teradata.tpcds.row.generator.RowGeneratorResult;

public class TpcdsRowReader implements
    org.apache.hadoop.mapred.RecordReader<NullWritable, TpcdsRow> {

  private final TpcdsSplit split;
  private final Session session;
  private final Table table;
  private RowGenerator rowGenerator;
  private final long startingRowNumber;
  private final long endingRowNumber;
  private long rowNumber;
  private RowGenerator parentRowGenerator;
  
  public TpcdsRowReader(TpcdsSplit split, Configuration jobConf) {
    this.split = split;
    this.session = split.getSession();
    this.table = Table.getTable(split.getTable());
    ChunkBoundaries chunkBoundaries = splitWork(table, session);
    this.startingRowNumber = chunkBoundaries.getFirstRow();
    this.endingRowNumber = chunkBoundaries.getLastRow();
    this.rowNumber = startingRowNumber;
    try {
      this.parentRowGenerator = table.isChild() ? table.getParent().getRowGeneratorClass().getDeclaredConstructor().newInstance() : null;
      this.rowGenerator = table.getRowGeneratorClass().getDeclaredConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      throw new TpcdsException(e.toString());
    }
    
    this.rowGenerator.skipRowsUntilStartingRowNumber(startingRowNumber);
  }

  @Override
  public void close() throws IOException {
    if (this.rowGenerator != null) {
      this.rowGenerator.consumeRemainingSeedsForRow();
    }
  }

  @Override
  public boolean next(NullWritable key, TpcdsRow value)
      throws IOException {
    
    List<TableRow> rows = computeNext();
    if (rows == null || rows.isEmpty()) {
      return false;
    }
    value.assign(rows.get(0));
    return true;
  }
  
  protected List<TableRow> computeNext()
  {
      if (rowNumber > endingRowNumber) {
          return null;
      }

      RowGeneratorResult result = rowGenerator.generateRowAndChildRows(rowNumber, session, parentRowGenerator, null);
      List<TableRow> tableRows = result.getRowAndChildRows();

      if (result.shouldEndRow()) {
          rowStop();
          rowNumber++;
      }

      if (result.getRowAndChildRows().isEmpty()) {
          tableRows = computeNext();
      }

      return tableRows;
  }

  private void rowStop()
  {
      rowGenerator.consumeRemainingSeedsForRow();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public TpcdsRow createValue() {
    return new TpcdsRow(this.table);
  }

  @Override
  public long getPos() throws IOException {
    return rowNumber;
  }

  @Override
  public float getProgress() throws IOException {
    return (endingRowNumber - rowNumber) / (endingRowNumber - startingRowNumber);
  }
}
