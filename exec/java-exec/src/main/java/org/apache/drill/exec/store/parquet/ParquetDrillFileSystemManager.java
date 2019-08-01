package org.apache.drill.exec.store.parquet;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Creates file system only if it was not created before, otherwise returns already created instance.
 */
class ParquetDrillFileSystemManager extends AbstractParquetScanBatchCreator.AbstractDrillFileSystemManager {

  private final boolean useAsyncPageReader;
  private DrillFileSystem fs;

  ParquetDrillFileSystemManager(OperatorContext operatorContext, boolean useAsyncPageReader) {
    super(operatorContext);
    this.useAsyncPageReader = useAsyncPageReader;
  }

  @Override
  protected DrillFileSystem get(Configuration config, Path path) throws ExecutionSetupException {
    if (fs == null) {
      try {
        fs =  useAsyncPageReader ? operatorContext.newNonTrackingFileSystem(config) : operatorContext.newFileSystem(config);
      } catch (IOException e) {
        throw new ExecutionSetupException(String.format("Failed to create DrillFileSystem: %s", e.getMessage()), e);
      }
    }
    return fs;
  }
}
