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

    test("DROP PLUGIN IF EXISTS smp");

    testBuilder()
      .sqlQuery("CREATE PLUGIN smp USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SELECT * FROM smp.`storage/sample-data.json`")
      .unOrdered()
      .baselineColumns("field1")
      .baselineValues("test")
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN smp", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' deleted successfully.")
      .go();

    errorMsgTestHelper("SELECT * FROM smp.`storage/sample-data.json`",
      "VALIDATION ERROR: Schema [[smp]] is not valid with respect to either root schema or current default schema.");
  }

  @Test
  public void testCreate() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN smp USING '%s'", config);

    test("DROP PLUGIN IF EXISTS smp");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' created successfully.")
      .go();

    errorMsgTestHelper(query, "PLAN ERROR: A plugin with given name [smp] already exists.");

    testBuilder()
      .sqlQuery("CREATE PLUGIN IF NOT EXISTS smp USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "A plugin with given name [smp] already exists.")
      .go();

    testBuilder()
      .sqlQuery("CREATE OR REPLACE PLUGIN smp USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' replaced successfully.")
      .go();


    testBuilder()
      .sqlQuery("DROP PLUGIN smp", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' deleted successfully.")
      .go();
  }

  @Test
  public void testDrop() throws Exception {
    test("DROP PLUGIN IF EXISTS smp");

    testBuilder()
      .sqlQuery("DROP PLUGIN IF EXISTS smp")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "Plugin [smp] not found.")
      .go();

    errorMsgTestHelper("DROP PLUGIN smp", "PLAN ERROR: Plugin [smp] not found.");
  }

  @Test
  public void testShow() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN smp USING '%s'", config);

    test("DROP PLUGIN IF EXISTS smp");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SHOW PLUGIN smp")
      .expectsNumRecords(1)
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN smp", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'smp' deleted successfully.")
      .go();


    testBuilder()
      .sqlQuery("SHOW PLUGIN smp")
      .unOrdered()
      .expectsEmptyResultSet()
      .go();
  }
}
