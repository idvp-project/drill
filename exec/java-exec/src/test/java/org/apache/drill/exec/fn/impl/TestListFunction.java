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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Category({SqlFunctionTest.class})
public class TestListFunction extends ClusterTest {
  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testListInteger() throws Exception {
    RowSet actual = getActualResult("select list(p.col_int) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_int from cp.`parquet/alltypes_required.parquet` p", Integer.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListInteger_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_int) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_int from cp.`parquet/alltypes_optional.parquet` p", Integer.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListFloat4() throws Exception {
    RowSet actual = getActualResult("select list(p.col_flt) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_flt from cp.`parquet/alltypes_required.parquet` p", Double.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListFloat4_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_flt) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_flt from cp.`parquet/alltypes_optional.parquet` p", Double.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListBit() throws Exception {
    RowSet actual = getActualResult("select list(p.col_bln) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_bln from cp.`parquet/alltypes_required.parquet` p", Boolean.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListBit_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_bln) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_bln from cp.`parquet/alltypes_optional.parquet` p", Boolean.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListTime() throws Exception {
    RowSet actual = getActualResult("select list(p.col_tim) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_tim from cp.`parquet/alltypes_required.parquet` p", Integer.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListTime_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_tim) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_tim from cp.`parquet/alltypes_optional.parquet` p", Integer.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListDate() throws Exception {
    RowSet actual = getActualResult("select list(p.col_dt) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_dt from cp.`parquet/alltypes_required.parquet` p", Long.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListDate_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_dt) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_dt from cp.`parquet/alltypes_optional.parquet` p", Long.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListTimeStamp() throws Exception {
    RowSet actual = getActualResult("select list(p.col_tmstmp) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_tmstmp from cp.`parquet/alltypes_required.parquet` p", Long.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListTimeStamp_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_tmstmp) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_tmstmp from cp.`parquet/alltypes_optional.parquet` p", Long.TYPE);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListVarDecimal() throws Exception {
    RowSet actual = getActualResult("select list(p.employee_id) as col from cp.`parquet/varlenDecimal.parquet` p");
    RowSet expected = getExpectedResult("select p.employee_id as varchar from cp.`parquet/varlenDecimal.parquet` p", BigDecimal.class);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListVarDecimal_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.manager_id) as col from cp.`parquet/varlenDecimal.parquet` p");
    RowSet expected = getExpectedResult("select p.manager_id from cp.`parquet/varlenDecimal.parquet` p", BigDecimal.class);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListInterval() throws Exception {
    RowSet actual = getActualResult("select list(p.col_intrvl_day) as col from cp.`parquet/alltypes_required.parquet` p");
    RowSet expected = getExpectedResult("select p.col_intrvl_day from cp.`parquet/alltypes_required.parquet` p", Period.class);
    RowSetUtilities.verify(expected, actual);
  }

  @Test
  public void testListInterval_nullable() throws Exception {
    RowSet actual = getActualResult("select list(p.col_intrvl_day) as col from cp.`parquet/alltypes_optional.parquet` p");
    RowSet expected = getExpectedResult("select p.col_intrvl_day from cp.`parquet/alltypes_optional.parquet` p", Period.class);
    RowSetUtilities.verify(expected, actual);
  }

  private RowSet getActualResult(String sql) throws Exception {
    return queryBuilder().sql(sql).rowSet();
  }

  private RowSet getExpectedResult(String sql, Class<?> arrayElementType) throws Exception {
    RowSet rowSet = queryBuilder().sql(sql).rowSet();
    try {

      MaterializedField column = rowSet.schema().column(0);

      TupleMetadata expectedSchema = new SchemaBuilder().addArray("col",
        column.getType().getMinorType(), column.getPrecision(), column.getScale()).buildSchema();

      List<Object> values = new ArrayList<>(rowSet.rowCount());
      RowSetReader reader = rowSet.reader();
      while (reader.next()) {
        if (reader.column(0).isNull()) {
          continue;
        }
        values.add(reader.column(0).getObject());
      }

      Object array = Array.newInstance(arrayElementType, values.size());
      for (int i = 0; i < values.size(); i++) {
        Array.set(array, i, values.get(i));
      }

      RowSetBuilder expected = client.rowSetBuilder(expectedSchema).addRow(array);
      return expected.build();

    } finally {
      rowSet.clear();
    }
  }
}
