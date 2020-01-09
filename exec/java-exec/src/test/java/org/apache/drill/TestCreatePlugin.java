package org.apache.drill;

import org.apache.drill.categories.SqlTest;
import org.apache.drill.test.BaseTestQuery;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author ozinoviev
 * @since 17.09.18
 */
@Category(SqlTest.class)
public class TestCreatePlugin extends PlanTestBase {

  @Test
  public void testCreateSelectAndDrop() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    test("DROP PLUGIN IF EXISTS sample");

    testBuilder()
      .sqlQuery("CREATE PLUGIN sample USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SELECT * FROM sample.`storage/sample-data.json`")
      .unOrdered()
      .baselineColumns("field1")
      .baselineValues("test")
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN sample", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' deleted successfully.")
      .go();

    errorMsgTestHelper("SELECT * FROM sample.`storage/sample-data.json`",
      "VALIDATION ERROR: Schema [[sample]] is not valid with respect to either root schema or current default schema.");
  }

  @Test
  public void testCreate() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN sample USING '%s'", config);

    test("DROP PLUGIN IF EXISTS sample");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' created successfully.")
      .go();

    errorMsgTestHelper(query, "PLAN ERROR: A plugin with given name [sample] already exists.");

    testBuilder()
      .sqlQuery("CREATE PLUGIN IF NOT EXISTS sample USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "A plugin with given name [sample] already exists.")
      .go();

    testBuilder()
      .sqlQuery("CREATE OR REPLACE PLUGIN sample USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' replaced successfully.")
      .go();


    testBuilder()
      .sqlQuery("DROP PLUGIN sample", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' deleted successfully.")
      .go();
  }

  @Test
  public void testDrop() throws Exception {
    test("DROP PLUGIN IF EXISTS sample");

    testBuilder()
      .sqlQuery("DROP PLUGIN IF EXISTS sample")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "Plugin [sample] not found.")
      .go();

    errorMsgTestHelper("DROP PLUGIN sample", "PLAN ERROR: Plugin [sample] not found.");
  }

  @Test
  public void testShow() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN sample USING '%s'", config);

    test("DROP PLUGIN IF EXISTS sample");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SHOW PLUGIN sample")
      .expectsNumRecords(1)
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN sample", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'sample' deleted successfully.")
      .go();


    testBuilder()
      .sqlQuery("SHOW PLUGIN sample")
      .unOrdered()
      .expectsEmptyResultSet()
      .go();
  }
}
