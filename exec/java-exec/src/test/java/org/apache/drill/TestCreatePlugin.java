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

    test("DROP PLUGIN IF EXISTS some_plugin");

    testBuilder()
      .sqlQuery("CREATE PLUGIN some_plugin USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SELECT * FROM some_plugin.`storage/sample-data.json`")
      .unOrdered()
      .baselineColumns("field1")
      .baselineValues("test")
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN some_plugin", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' deleted successfully.")
      .go();

    errorMsgTestHelper("SELECT * FROM some_plugin.`storage/sample-data.json`",
      "VALIDATION ERROR: Schema [[some_plugin]] is not valid with respect to either root schema or current default schema.");
  }

  @Test
  public void testCreate() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN some_plugin USING '%s'", config);

    test("DROP PLUGIN IF EXISTS some_plugin");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' created successfully.")
      .go();

    errorMsgTestHelper(query, "PLAN ERROR: A plugin with given name [some_plugin] already exists.");

    testBuilder()
      .sqlQuery("CREATE PLUGIN IF NOT EXISTS some_plugin USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "A plugin with given name [some_plugin] already exists.")
      .go();

    testBuilder()
      .sqlQuery("CREATE OR REPLACE PLUGIN some_plugin USING '%s'", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' replaced successfully.")
      .go();


    testBuilder()
      .sqlQuery("DROP PLUGIN some_plugin", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' deleted successfully.")
      .go();
  }

  @Test
  public void testDrop() throws Exception {
    test("DROP PLUGIN IF EXISTS some_plugin");

    testBuilder()
      .sqlQuery("DROP PLUGIN IF EXISTS some_plugin")
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(false, "Plugin [some_plugin] not found.")
      .go();

    errorMsgTestHelper("DROP PLUGIN some_plugin", "PLAN ERROR: Plugin [some_plugin] not found.");
  }

  @Test
  public void testShow() throws Exception {
    String config = BaseTestQuery.getFile("storage/sample-storage.json");

    String query = String.format("CREATE PLUGIN some_plugin USING '%s'", config);

    test("DROP PLUGIN IF EXISTS some_plugin");

    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' created successfully.")
      .go();

    testBuilder()
      .sqlQuery("SHOW PLUGIN some_plugin")
      .expectsNumRecords(1)
      .go();

    testBuilder()
      .sqlQuery("DROP PLUGIN some_plugin", config)
      .unOrdered()
      .baselineColumns("ok", "summary")
      .baselineValues(true, "Plugin 'some_plugin' deleted successfully.")
      .go();


    testBuilder()
      .sqlQuery("SHOW PLUGIN some_plugin")
      .unOrdered()
      .expectsEmptyResultSet()
      .go();
  }
}
