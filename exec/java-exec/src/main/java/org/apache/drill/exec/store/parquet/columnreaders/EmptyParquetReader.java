/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.CommonParquetRecordReader;
import org.apache.drill.exec.store.parquet.ParquetRowGroupScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.ArrayList;

/**
 * Reader for empty parquet files.
 * Provides only schema information from parquet file footer.
 */
public class EmptyParquetReader extends CommonParquetRecordReader {

  public EmptyParquetReader(final ParquetMetadata footer,
                            final FragmentContext fragmentContext) {
    super(footer, fragmentContext);
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
