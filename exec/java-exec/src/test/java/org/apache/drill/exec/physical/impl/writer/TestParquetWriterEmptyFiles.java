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
package org.apache.drill.exec.physical.impl.writer;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchemaBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.categories.ParquetTest;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.exec.ExecConstants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

@Category({ParquetTest.class, UnlikelyTest.class})
public class TestParquetWriterEmptyFiles extends BaseTestQuery {

  @BeforeClass
  public static void initFs() throws Exception {
    updateTestCluster(3, null);
  }

  @Test // see DRILL-2408
  public void testWriteEmptyFile() throws Exception {
    final String outputFileName = "testparquetwriteremptyfiles_testwriteemptyfile";
    final File outputFile = FileUtils.getFile(dirTestWatcher.getDfsTestTmpDir(), outputFileName);

    test("CREATE TABLE dfs.tmp.%s AS SELECT * FROM cp.`employee.json` WHERE 1=0", outputFileName);
    Assert.assertTrue(outputFile.exists());
  }

  @Test
  public void testEmptyFileSchema() throws Exception {
    final String outputFileName = "testparquetwriteremptyfiles_testemptyfileschema";

    test("CREATE TABLE dfs.tmp.%s AS SELECT * FROM cp.`employee.json` WHERE 1=0", outputFileName);

    // end_date column is null, so it missing in result schema.
    SchemaBuilder schemaBuilder = new SchemaBuilder()
            .addNullable("employee_id", TypeProtos.MinorType.BIGINT)
            .addNullable("full_name", TypeProtos.MinorType.VARCHAR)
            .addNullable("first_name", TypeProtos.MinorType.VARCHAR)
            .addNullable("last_name", TypeProtos.MinorType.VARCHAR)
            .addNullable("position_id", TypeProtos.MinorType.BIGINT)
            .addNullable("position_title", TypeProtos.MinorType.VARCHAR)
            .addNullable("store_id", TypeProtos.MinorType.BIGINT)
            .addNullable("department_id", TypeProtos.MinorType.BIGINT)
            .addNullable("birth_date", TypeProtos.MinorType.VARCHAR)
            .addNullable("hire_date", TypeProtos.MinorType.VARCHAR)
            .addNullable("salary", TypeProtos.MinorType.FLOAT8)
            .addNullable("supervisor_id", TypeProtos.MinorType.BIGINT)
            .addNullable("education_level", TypeProtos.MinorType.VARCHAR)
            .addNullable("marital_status", TypeProtos.MinorType.VARCHAR)
            .addNullable("gender", TypeProtos.MinorType.VARCHAR)
            .addNullable("management_role", TypeProtos.MinorType.VARCHAR);
    BatchSchema expectedSchema = new BatchSchemaBuilder()
            .withSchemaBuilder(schemaBuilder)
            .build();

    testBuilder()
            .unOrdered()
            .sqlQuery("select * from dfs.tmp.%s", outputFileName)
            .schemaBaseLine(expectedSchema)
            .go();
  }

  @Test
  public void testMultipleWriters() throws Exception {
    final String outputFile = "testparquetwriteremptyfiles_testmultiplewriters";

    runSQL("alter session set `planner.slice_target` = 1");

    try {
      final String query = "SELECT position_id FROM cp.`employee.json` WHERE position_id IN (15, 16) GROUP BY position_id";

      test("CREATE TABLE dfs.tmp.%s AS %s", outputFile, query);

      // this query will fail if an "empty" file was created
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM dfs.tmp.%s", outputFile)
        .sqlBaselineQuery(query)
        .go();
    } finally {
      runSQL("alter session set `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test // see DRILL-2408
  public void testWriteEmptyFileAfterFlush() throws Exception {
    final String outputFileName = "testparquetwriteremptyfiles_test_write_empty_file_after_flush";
    final File outputFile = FileUtils.getFile(dirTestWatcher.getDfsTestTmpDir(), outputFileName);

    try {
      // this specific value will force a flush just after the final row is written
      // this may cause the creation of a new "empty" parquet file
      test("ALTER SESSION SET `store.parquet.block-size` = 19926");

      final String query = "SELECT * FROM cp.`employee.json` LIMIT 100";
      test("CREATE TABLE dfs.tmp.%s AS %s", outputFileName, query);

      // Make sure that only 1 parquet file was created
      Assert.assertEquals(1, outputFile.list((dir, name) -> name.endsWith("parquet")).length);

      // this query will fail if an "empty" file was created
      testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM dfs.tmp.%s", outputFileName)
        .sqlBaselineQuery(query)
        .go();
    } finally {
      // restore the session option
      resetSessionOption(ExecConstants.PARQUET_BLOCK_SIZE);
    }
  }
}
