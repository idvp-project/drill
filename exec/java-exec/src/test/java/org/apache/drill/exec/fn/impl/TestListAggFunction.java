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
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SqlFunctionTest.class})
public class TestListAggFunction extends ClusterTest {
    @BeforeClass
    public static void setup() throws Exception {
        ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
        startCluster(builder);
    }

    @Test
    public void testListAggInteger() throws Exception {
        String actual = getActualResult("select listagg(p.col_int) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_int as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInteger_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_int, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_int as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInteger_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_int) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_int as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInteger_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_int, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_int as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggFloat() throws Exception {
        String actual = getActualResult("select listagg(p.col_flt) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_flt as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggFloat_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_flt, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_flt as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggFloat_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_flt) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_flt as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggFloat_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_flt, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_flt as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggBit() throws Exception {
        String actual = getActualResult("select listagg(p.col_bln) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_bln as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggBit_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_bln, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_bln as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggBit_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_bln) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_bln as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggBit_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_bln, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_bln as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTime() throws Exception {
        String actual = getActualResult("select listagg(p.col_tim) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tim as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTime_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_tim, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tim as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTime_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_tim) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tim as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTime_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_dt, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_dt as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggDate() throws Exception {
        String actual = getActualResult("select listagg(p.col_dt) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_dt as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggDate_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_dt, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_dt as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggDate_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_dt) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_dt as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggDate_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_dt, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_dt as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTimeStamp() throws Exception {
        String actual = getActualResult("select listagg(p.col_tmstmp) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tmstmp as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTimeStamp_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_tmstmp, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tmstmp as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTimeStamp_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_tmstmp) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tmstmp as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggTimeStamp_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_tmstmp, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_tmstmp as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select p.col_vrchr from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select p.col_vrchr from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select p.col_vrchr from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select p.col_vrchr from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar_empty() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select coalesce(p.col_vrchr, '') from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarChar_empty_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_vrchr, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select coalesce(p.col_vrchr, '') from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarDecimal() throws Exception {
        String actual = getActualResult("select listagg(p.employee_id) from cp.`parquet/varlenDecimal.parquet` p");
        String expected = getExpectedResult("select cast(p.employee_id as varchar) from cp.`parquet/varlenDecimal.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarDecimal_separated() throws Exception {
        String actual = getActualResult("select listagg(p.employee_id, ';') from cp.`parquet/varlenDecimal.parquet` p");
        String expected = getExpectedResult("select  cast(p.employee_id as varchar) from cp.`parquet/varlenDecimal.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarDecimal_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.manager_id) from cp.`parquet/varlenDecimal.parquet` p");
        String expected = getExpectedResult("select cast(p.manager_id as varchar) from cp.`parquet/varlenDecimal.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggVarDecimal_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.manager_id, ';') from cp.`parquet/varlenDecimal.parquet` p");
        String expected = getExpectedResult("select cast(p.manager_id as varchar) from cp.`parquet/varlenDecimal.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInterval() throws Exception {
        String actual = getActualResult("select listagg(p.col_intrvl_day) from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select cast(p.col_intrvl_day as varchar) from cp.`parquet/alltypes_required.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInterval_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_intrvl_day, ';') from cp.`parquet/alltypes_required.parquet` p");
        String expected = getExpectedResult("select  cast(p.col_intrvl_day as varchar) from cp.`parquet/alltypes_required.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInterval_nullable() throws Exception {
        String actual = getActualResult("select listagg(p.col_intrvl_day) from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_intrvl_day as varchar) from cp.`parquet/alltypes_optional.parquet` p", "");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testListAggInterval_nullable_separated() throws Exception {
        String actual = getActualResult("select listagg(p.col_intrvl_day, ';') from cp.`parquet/alltypes_optional.parquet` p");
        String expected = getExpectedResult("select cast(p.col_intrvl_day as varchar) from cp.`parquet/alltypes_optional.parquet` p", ";");
        Assert.assertEquals(expected, actual);
    }

    private String getActualResult(String sql) throws Exception {
        RowSet rowSet = queryBuilder().sql(sql).rowSet();
        try {
            RowSetReader reader = rowSet.reader();
            Assert.assertTrue(reader.next());
            if (reader.column(0).isNull()) {
                return null;
            }

            return reader.column(0).scalar().getString();
        } finally {
            rowSet.clear();
        }
    }

    private String getExpectedResult(String sql, String separator) throws Exception {
        RowSet rowSet = queryBuilder().sql(sql).rowSet();
        try {
            boolean empty = true;
            RowSetReader reader = rowSet.reader();
            StringBuilder sb = new StringBuilder();
            while (reader.next()) {
                if (reader.column(0).isNull()) {
                    continue;
                }
                String value = reader.column(0).scalar().getString();
                if (value.isEmpty()) {
                    continue;
                }

                empty = false;
                if (sb.length() > 0) {
                    sb.append(separator);
                }

                sb.append(value);
            }

            if (empty) {
                return null;
            } else {
                return sb.toString();
            }
        } finally {
            rowSet.clear();
        }
    }
}
