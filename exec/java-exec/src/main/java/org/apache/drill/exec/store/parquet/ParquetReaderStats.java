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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.store.CommonParquetRecordReader.Metric;
import org.apache.hadoop.fs.Path;

public class ParquetReaderStats {

  public final AtomicLong numRowgroups = new AtomicLong();
  public final AtomicLong rowgroupsPruned = new AtomicLong();

  public final AtomicLong numDictPageLoads = new AtomicLong();
  public final AtomicLong numDataPageLoads = new AtomicLong();
  public final AtomicLong numDataPagesDecoded = new AtomicLong();
  public final AtomicLong numDictPagesDecompressed = new AtomicLong();
  public final AtomicLong numDataPagesDecompressed = new AtomicLong();

  public final AtomicLong totalDictPageReadBytes = new AtomicLong();
  public final AtomicLong totalDataPageReadBytes = new AtomicLong();
  public final AtomicLong totalDictDecompressedBytes = new AtomicLong();
  public final AtomicLong totalDataDecompressedBytes = new AtomicLong();

  public final AtomicLong timeDictPageLoads = new AtomicLong();
  public final AtomicLong timeDataPageLoads = new AtomicLong();
  public final AtomicLong timeDataPageDecode = new AtomicLong();
  public final AtomicLong timeDictPageDecode = new AtomicLong();
  public final AtomicLong timeDictPagesDecompressed = new AtomicLong();
  public final AtomicLong timeDataPagesDecompressed = new AtomicLong();

  public final AtomicLong timeDiskScanWait = new AtomicLong();
  public final AtomicLong timeDiskScan = new AtomicLong();
  public final AtomicLong timeFixedColumnRead = new AtomicLong();
  public final AtomicLong timeVarColumnRead = new AtomicLong();
  public final AtomicLong timeProcess = new AtomicLong();

  public ParquetReaderStats() {
  }

  public void logStats(org.slf4j.Logger logger, Path hadoopPath) {
    logger.trace(
        "ParquetTrace,Summary,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        hadoopPath,
        numRowgroups,
        rowgroupsPruned,
        numDictPageLoads,
        numDataPageLoads,
        numDataPagesDecoded,
        numDictPagesDecompressed,
        numDataPagesDecompressed,
        totalDictPageReadBytes,
        totalDataPageReadBytes,
        totalDictDecompressedBytes,
        totalDataDecompressedBytes,
        timeDictPageLoads,
        timeDataPageLoads,
        timeDataPageDecode,
        timeDictPageDecode,
        timeDictPagesDecompressed,
        timeDataPagesDecompressed,
        timeDiskScanWait,
        timeDiskScan,
        timeFixedColumnRead,
        timeVarColumnRead
    );
  }

  public void update(OperatorStats stats){
    stats.setLongStat(Metric.NUM_ROWGROUPS,
        numRowgroups.longValue());
    stats.setLongStat(Metric.ROWGROUPS_PRUNED,
        rowgroupsPruned.longValue());
    stats.addLongStat(Metric.NUM_DICT_PAGE_LOADS,
        numDictPageLoads.longValue());
    stats.addLongStat(Metric.NUM_DATA_PAGE_lOADS, numDataPageLoads.longValue());
    stats.addLongStat(Metric.NUM_DATA_PAGES_DECODED, numDataPagesDecoded.longValue());
    stats.addLongStat(Metric.NUM_DICT_PAGES_DECOMPRESSED,
        numDictPagesDecompressed.longValue());
    stats.addLongStat(Metric.NUM_DATA_PAGES_DECOMPRESSED,
        numDataPagesDecompressed.longValue());
    stats.addLongStat(Metric.TOTAL_DICT_PAGE_READ_BYTES,
        totalDictPageReadBytes.longValue());
    stats.addLongStat(Metric.TOTAL_DATA_PAGE_READ_BYTES,
        totalDataPageReadBytes.longValue());
    stats.addLongStat(Metric.TOTAL_DICT_DECOMPRESSED_BYTES,
        totalDictDecompressedBytes.longValue());
    stats.addLongStat(Metric.TOTAL_DATA_DECOMPRESSED_BYTES,
        totalDataDecompressedBytes.longValue());
    stats.addLongStat(Metric.TIME_DICT_PAGE_LOADS,
        timeDictPageLoads.longValue());
    stats.addLongStat(Metric.TIME_DATA_PAGE_LOADS,
        timeDataPageLoads.longValue());
    stats.addLongStat(Metric.TIME_DATA_PAGE_DECODE,
        timeDataPageDecode.longValue());
    stats.addLongStat(Metric.TIME_DICT_PAGE_DECODE,
        timeDictPageDecode.longValue());
    stats.addLongStat(Metric.TIME_DICT_PAGES_DECOMPRESSED,
        timeDictPagesDecompressed.longValue());
    stats.addLongStat(Metric.TIME_DATA_PAGES_DECOMPRESSED,
        timeDataPagesDecompressed.longValue());
    stats.addLongStat(Metric.TIME_DISK_SCAN_WAIT,
        timeDiskScanWait.longValue());
    stats.addLongStat(Metric.TIME_DISK_SCAN, timeDiskScan.longValue());
    stats.addLongStat(Metric.TIME_FIXEDCOLUMN_READ, timeFixedColumnRead.longValue());
    stats.addLongStat(Metric.TIME_VARCOLUMN_READ, timeVarColumnRead.longValue());
    stats.addLongStat(Metric.TIME_PROCESS, timeProcess.longValue());
  }
}
