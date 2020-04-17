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
package org.apache.drill.exec.fn.impl;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;

@Category(SqlFunctionTest.class)
public class TestNewSimpleRepeatedFunctions extends ClusterTest {

  private static final String REPEATED_TYPES_PARQUET_TABLE = "cp.`store/parquet/complex/repeated_types.parquet`";
  private static final String REPEATED_TYPES_JSON_TABLE = "cp.`parquet/alltypes_repeated.json`";
  private static final String SELECT_REPEATED_COUNT_LIST = "select repeated_count(array) from dfs.`functions/repeated/repeated_list.json`";
  private static final String SELECT_REPEATED_COUNT_MAP = "select repeated_count(mapArray) from dfs.`functions/repeated/repeated_map.json`";
  private static final String SELECT_REPEATED_COUNT_QUERY = "select repeated_count(%s) from %s";
  private static final String COLUMN_NAME = "EXPR$0";

  @BeforeClass
  public static void setUp() throws Exception {
    dirTestWatcher.copyResourceToRoot(Paths.get("functions", "repeated"));
    startCluster(ClusterFixture.builder(dirTestWatcher));
  }

  @Test
  public void testRepeatedContainsForWildCards() throws Exception {
    String query = "select repeated_contains(topping, '%s*') from cp.`testRepeatedWrite.json`";
    testBuilder()
        .sqlQuery(query, "Choc")
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(true, true, true, true, false)
        .go();

    testBuilder()
        .sqlQuery(query, "Pow")
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(true, false, false, true, false)
        .go();
  }

  @Test
  public void testRepeatedCountVarCharJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "str_list", "cp.`store/json/json_basic_repeated_varchar.json`")
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(5, 1, 3, 1)
        .go();
  }

  @Test
  public void testRepeatedUnionVarCharJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(str_list, str_list)", "cp.`store/json/json_basic_repeated_varchar.json`")
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(10, 2, 6, 2)
        .go();
  }

  @Test
  public void testRepeatedDistinctVarCharJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(str_list, str_list))", "cp.`store/json/json_basic_repeated_varchar.json`")
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(4, 1, 3, 1)
        .go();
  }

  @Test
  public void testRepeatedCountIntJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int_col", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(12, 4, 4, 4)
        .go();
  }

  @Test
  public void testRepeatedUnionIntJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int_col, int_col)", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(24, 8, 8, 8)
        .go();
  }

  @Test
  public void testRepeatedDistinctIntJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int_col, int_col))", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(12, 4, 4, 4)
        .go();
  }

  @Test
  public void testRepeatedFloatJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "float4_col", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(7, 4, 4, 4)
        .go();
  }

  @Test
  public void testRepeatedUnionFloatJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(float4_col, float4_col)", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(14, 8, 8, 8)
        .go();
  }

  @Test
  public void testRepeatedDistinctFloatJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(float4_col, float4_col))", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(7, 4, 4, 4)
        .go();
  }

  @Test
  public void testRepeatedBitJSON() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "bit_col", REPEATED_TYPES_JSON_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(7, 7, 5, 3)
        .go();
  }

  @Test
  public void testRepeatedCountDate() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "date_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 1, 0)
        .go();
  }

  @Test
  public void testRepeatedUnionDate() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(date_list, date_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(6, 2, 0)
        .go();
  }

  @Test
  public void testRepeatedDistinctDate() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(date_list, date_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 1, 0)
        .go();
  }

  @Test
  public void testRepeatedCountTime() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "time_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 4, 3)
        .go();
  }

  @Test
  public void testRepeatedUnionTime() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(time_list, time_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 8, 6)
        .go();
  }

  @Test
  public void testRepeatedDistinctTime() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(time_list, time_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountTimestamp() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "timestamp_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 2, 5)
        .go();
  }

  @Test
  public void testRepeatedUnionTimestamp() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(timestamp_list, timestamp_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 4, 10)
        .go();
  }

  @Test
  public void testRepeatedDistinctTimestamp() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(timestamp_list, timestamp_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 2, 1)
        .go();
  }

  @Test
  public void testRepeatedCountInterval() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "interval_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 5, 0)
        .go();
  }

  @Test
  public void testRepeatedUnionInterval() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(interval_list, interval_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(4, 10, 0)
        .go();
  }

  @Test
  public void testRepeatedDistinctInterval() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(interval_list, interval_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 0)
        .go();
  }

  @Test
  public void testRepeatedCountVarChar() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "string_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 3, 10)
        .go();
  }

  @Test
  public void testRepeatedUnionVarChar() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(string_list, string_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 6, 20)
        .go();
  }

  @Test
  public void testRepeatedDistinctVarChar() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(string_list, string_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int8_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 1, 3)
        .go();
  }

  @Test
  public void testRepeatedUnionInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int8_list, int8_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(6, 2, 6)
        .go();
  }

  @Test
  public void testRepeatedDistinctInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int8_list, int8_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int16_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 2, 4)
        .go();
  }

  @Test
  public void testRepeatedUnionInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int16_list, int16_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(6, 4, 8)
        .go();
  }

  @Test
  public void testRepeatedDistinctInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int16_list, int16_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int32_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 9, 3)
        .go();
  }

  @Test
  public void testRepeatedUnionInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int32_list, int32_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 18, 6)
        .go();
  }

  @Test
  public void testRepeatedDistinctInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int32_list, int32_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountUInt8() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "uint8_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 7, 1)
        .go();
  }

  @Test
  public void testRepeatedUnionUInt8() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(uint8_list, uint8_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 14, 2)
        .go();
  }

  @Test
  public void testRepeatedDistinctUInt8() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(uint8_list, uint8_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountUInt16() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "uint16_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 3, 1)
        .go();
  }

  @Test
  public void testRepeatedUnionUInt16() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(uint16_list, uint16_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 6, 2)
        .go();
  }

  @Test
  public void testRepeatedDistinctUInt16() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(uint16_list, uint16_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountUInt32() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "uint32_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(4, 1, 4)
        .go();
  }

  @Test
  public void testRepeatedUnionUInt32() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(uint32_list, uint32_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(8, 2, 8)
        .go();
  }

  @Test
  public void testRepeatedDistinctUInt32() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(uint32_list, uint32_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountBigInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int64_raw_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(4, 1, 2)
        .go();
  }

  @Test
  public void testRepeatedUnionBigInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int64_raw_list, int64_raw_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(8, 2, 4)
        .go();
  }

  @Test
  public void testRepeatedDistinctBigInt() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int64_raw_list, int64_raw_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountBigInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int64_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(9, 2, 4)
        .go();
  }

  @Test
  public void testRepeatedUnionBigInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int64_list, int64_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(18, 4, 8)
        .go();
  }

  @Test
  public void testRepeatedDistinctBigInt_2() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int64_list, int64_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountBigInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "uint64_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 3)
        .go();
  }

  @Test
  public void testRepeatedUnionBigInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(uint64_list, uint64_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 2, 6)
        .go();
  }

  @Test
  public void testRepeatedDistinctBigInt_3() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(uint64_list, uint64_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountVarDecimal() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "decimal_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(7, 3, 6)
        .go();
  }

  @Test
  public void testRepeatedUnionVarDecimal() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(decimal_list, decimal_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(14, 6, 12)
        .go();
  }

  @Test
  public void testRepeatedDistinctVarDecimal() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(decimal_list, decimal_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(1, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountVarBinary() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "int96_raw_list", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 9)
        .go();
  }

  @Test
  public void testRepeatedUnionVarBinary() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_union(int96_raw_list, int96_raw_list)", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 2, 18)
        .go();
  }

  @Test
  public void testRepeatedDistinctVarBinary() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_QUERY, "repeated_distinct(repeated_union(int96_raw_list, int96_raw_list))", REPEATED_TYPES_PARQUET_TABLE)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(0, 1, 1)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedMap() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_MAP)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 2, 3, 0, 1, 5, 2)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedMapInWhere() throws Exception {
    String query = SELECT_REPEATED_COUNT_MAP + " where repeated_count(mapArray) > 2";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 5)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedMapInHaving() throws Exception {
    String query = SELECT_REPEATED_COUNT_MAP + " group by 1 having repeated_count(mapArray) < 3";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(2, 0, 1)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedList() throws Exception {
    testBuilder()
        .sqlQuery(SELECT_REPEATED_COUNT_LIST)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 0, 2, 5, 9, 4, 0, 3)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedListInWhere() throws Exception {
    String query = SELECT_REPEATED_COUNT_LIST + " where repeated_count(array) > 4";
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(5, 9)
        .go();
  }

  @Test
  public void testRepeatedCountRepeatedListInHaving() throws Exception {
    String query = SELECT_REPEATED_COUNT_LIST + " group by 1 having repeated_count(array) < 4";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(COLUMN_NAME)
        .baselineValuesForSingleColumn(3, 0, 2)
        .go();
  }
}
