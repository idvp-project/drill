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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Set;

// Class containing information for reading a empty parquet file
@JsonTypeName("empty-parquet-row-group-scan")
public class EmptyParquetRowGroupScan extends AbstractParquetRowGroupScan {

  private final ParquetFormatPlugin formatPlugin;
  private final ParquetFormatConfig formatConfig;
  private final Set<Path> files;

  @JsonCreator
  public EmptyParquetRowGroupScan(@JacksonInject StoragePluginRegistry registry,
                                  @JsonProperty("userName") String userName,
                                  @JsonProperty("files") Set<Path> files,
                                  @JsonProperty("storageConfig") StoragePluginConfig storageConfig,
                                  @JsonProperty("formatConfig") FormatPluginConfig formatConfig,
                                  @JsonProperty("columns") List<SchemaPath> columns,
                                  @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
                                  @JsonProperty("selectionRoot") Path selectionRoot,
                                  @JsonProperty("filter") LogicalExpression filter,
                                  @JsonProperty("schema") TupleMetadata schema) throws ExecutionSetupException {
    this(userName,
        (ParquetFormatPlugin) registry.getFormatPlugin(Preconditions.checkNotNull(storageConfig), Preconditions.checkNotNull(formatConfig)),
        files,
        columns,
        readerConfig,
        selectionRoot,
        filter,
        schema);
  }

  public EmptyParquetRowGroupScan(String userName,
                                  ParquetFormatPlugin formatPlugin,
                                  Set<Path> files,
                                  List<SchemaPath> columns,
                                  ParquetReaderConfig readerConfig,
                                  Path selectionRoot,
                                  LogicalExpression filter,
                                  TupleMetadata schema) {
    super(userName, ImmutableList.of(), columns, readerConfig, filter, selectionRoot, schema);
    this.files = Preconditions.checkNotNull(files);
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin, "Could not find format config for the given configuration");
    this.formatConfig = formatPlugin.getConfig();
  }

  @JsonProperty
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty
  public ParquetFormatConfig getFormatConfig() {
    return formatConfig;
  }

  @JsonIgnore
  public ParquetFormatPlugin getStorageEngine() {
    return formatPlugin;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new EmptyParquetRowGroupScan(getUserName(), formatPlugin, files, columns, readerConfig, selectionRoot, filter, schema);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

  @Override
  public AbstractParquetRowGroupScan copy(List<SchemaPath> columns) {
    return new EmptyParquetRowGroupScan(getUserName(), formatPlugin, files, columns, readerConfig, selectionRoot, filter, schema);
  }

  @Override
  public Configuration getFsConf(RowGroupReadEntry rowGroupReadEntry) {
    throw new UnsupportedOperationException("Empty parquet file");
  }

  @Override
  public boolean supportsFileImplicitColumns() {
    return selectionRoot != null;
  }

  @Override
  public List<String> getPartitionValues(RowGroupReadEntry rowGroupReadEntry) {
    return ImmutableList.of();
  }

  public Set<Path> getFiles() {
    return files;
  }
}

