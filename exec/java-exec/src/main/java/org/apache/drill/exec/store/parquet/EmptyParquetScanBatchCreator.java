package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.CloseableRecordBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.ColumnExplorer;
import org.apache.drill.exec.store.CommonParquetRecordReader;
import org.apache.drill.exec.store.parquet.columnreaders.EmptyParquetReader;
import org.apache.drill.shaded.guava.com.google.common.base.Functions;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class EmptyParquetScanBatchCreator extends AbstractParquetScanBatchCreator implements BatchCreator<EmptyParquetRowGroupScan> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmptyParquetScanBatchCreator.class);

  @Override
  public CloseableRecordBatch getBatch(ExecutorFragmentContext context, EmptyParquetRowGroupScan rowGroupScan, List<RecordBatch> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    OperatorContext oContext = context.newOperatorContext(rowGroupScan);

    final ColumnExplorer columnExplorer = new ColumnExplorer(context.getOptions(), rowGroupScan.getColumns());

    if (!columnExplorer.isStarQuery()) {
      rowGroupScan = (EmptyParquetRowGroupScan) rowGroupScan.copy(columnExplorer.getTableColumns());
      rowGroupScan.setOperatorId(rowGroupScan.getOperatorId());
    }

    List<CommonParquetRecordReader> readers = new LinkedList<>();
    List<Map<String, String>> implicitColumns = new ArrayList<>();
    Map<String, String> mapWithMaxColumns = new LinkedHashMap<>();

    Stopwatch timer = logger.isTraceEnabled() ? Stopwatch.createUnstarted() : null;

    try {

      for (Path file : rowGroupScan.getFiles()) {

        if (timer != null) {
          timer.start();
        }

        ParquetMetadata footer = readFooter(rowGroupScan.getStorageEngine().getFsConf(), file, rowGroupScan.getReaderConfig());
        if (timer != null) {
          long timeToRead = timer.elapsed(TimeUnit.MICROSECONDS);
          timer.stop().reset();
          logger.trace("ParquetTrace,Read Footer,{},{},{},{},{},{},{}", "", file, "", 0, 0, 0, timeToRead);
        }

        readers.add(new EmptyParquetReader(footer, context));

        List<String> partitionValues = ColumnExplorer.listPartitionValues(file, rowGroupScan.getSelectionRoot(), false);
        Map<String, String> implicitValues = columnExplorer.populateImplicitColumns(rowGroupScan.getSelectionRoot(), partitionValues, rowGroupScan.supportsFileImplicitColumns());
        implicitColumns.add(implicitValues);
        if (mapWithMaxColumns.size() < implicitValues.size()) {
          mapWithMaxColumns = implicitValues;
        }
      }
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }

    // all readers should have the same number of implicit columns, add missing ones with value null
    Map<String, String> diff = Maps.transformValues(mapWithMaxColumns, Functions.constant(null));
    for (Map<String, String> map : implicitColumns) {
      map.putAll(Maps.difference(map, diff).entriesOnlyOnRight());
    }

    return new ScanBatch(context, oContext, readers, implicitColumns);
  }

  @Override
  protected AbstractDrillFileSystemManager getDrillFileSystemCreator(OperatorContext operatorContext, OptionManager optionManager) {
    return new ParquetDrillFileSystemManager(operatorContext, optionManager.getOption(ExecConstants.PARQUET_PAGEREADER_ASYNC).bool_val);
  }
}
