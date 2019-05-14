package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.parquet.ParquetRowGroupScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.ArrayList;

/**
 * Reader for empty parquet files.
 * Provides only schema information from parquet file footer.
 *
 * @author ozinoviev
 * @since 14.05.19
 */
public class EmptyParquetReader extends AbstractRecordReader {

  private final ParquetMetadata footer;

  public EmptyParquetReader(final ParquetRowGroupScan scan,
                            final ParquetMetadata footer) {
    Preconditions.checkArgument(scan.getRowGroupReadEntries().isEmpty());
    this.footer = footer;
  }

  @Override
  public void setup(final OperatorContext context,
                    final OutputMutator output) throws ExecutionSetupException {

    if (footer == null) {
      return;
    }

    try {

      ParquetSchema schema = new ParquetSchema(context.getFragmentContext().getOptions(), 0, footer, isStarQuery() ? null : getColumns());
      schema.buildSchema();

      for (ParquetColumnMetadata columnMetadata : schema.getColumnMetadata()) {
        columnMetadata.buildVector(output);
      }

      if (!schema.isStarQuery()) {
        schema.createNonExistentColumns(output, new ArrayList<>());
      }

    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    return 0;
  }

  @Override
  public void close() throws Exception {

  }
}
