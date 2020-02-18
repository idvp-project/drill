package org.apache.drill.exec.udfs.datasketches;

import org.apache.drill.categories.SqlFunctionTest;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SqlFunctionTest.class)
public class ThetaFunctionsTest extends ClusterTest {
  @BeforeClass
  public static void setup() throws Exception {
    ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    startCluster(builder);
  }

  @Test
  public void testThetaCount() throws Exception {
    final String query = "select theta_count(`col_int`) as col_int, " +
      "theta_count(`col_dt`) as col_dt, " +
      "theta_count(`col_tmstmp`) as col_tmstmp, " +
      "theta_count(`col_vrchr`) as col_vrchr, " +
      "theta_count(`col_tim`) as col_tim, " +
      "theta_count(`col_flt`) as col_flt, " +
      "theta_count(`col_chr`) as col_chr " +
      "from cp.`parquet/alltypes_required.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(4L, 4L, 4L, 4L, 4L, 4L, 4L)
      .go();
  }

  @Test
  public void testTheta_decodeCount() throws Exception {
    final String query = "select theta_decode_count(theta(`col_int`)) as col_int, " +
      "theta_decode_count(theta(`col_dt`)) as col_dt, " +
      "theta_decode_count(theta(`col_tmstmp`)) as col_tmstmp, " +
      "theta_decode_count(theta(`col_vrchr`)) as col_vrchr, " +
      "theta_decode_count(theta(`col_tim`)) as col_tim, " +
      "theta_decode_count(theta(`col_flt`)) as col_flt, " +
      "theta_decode_count(theta(`col_chr`)) as col_chr " +
      "from cp.`parquet/alltypes_required.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(4L, 4L, 4L, 4L, 4L, 4L, 4L)
      .go();
  }

  @Test
  public void testTheta_decode() throws Exception {
    final String query = "select theta_decode(theta(`col_int`)) as col_int, " +
      "theta_decode(theta(`col_dt`)) as col_dt, " +
      "theta_decode(theta(`col_tmstmp`)) as col_tmstmp, " +
      "theta_decode(theta(`col_vrchr`)) as col_vrchr, " +
      "theta_decode(theta(`col_tim`)) as col_tim, " +
      "theta_decode(theta(`col_flt`)) as col_flt, " +
      "theta_decode(theta(`col_chr`)) as col_chr " +
      "from cp.`parquet/alltypes_required.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
      .go();
  }

  @Test
  public void testThetaCount_nullable() throws Exception {
    final String query = "select theta_count(`col_int`) as col_int, " +
      "theta_count(`col_dt`) as col_dt, " +
      "theta_count(`col_tmstmp`) as col_tmstmp, " +
      "theta_count(`col_vrchr`) as col_vrchr, " +
      "theta_count(`col_tim`) as col_tim, " +
      "theta_count(`col_flt`) as col_flt, " +
      "theta_count(`col_chr`) as col_chr " +
      "from cp.`parquet/alltypes_optional.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(2L, 3L, 3L, 3L, 3L, 3L, 3L)
      .go();
  }

  @Test
  public void testTheta_decodeCount_nullable() throws Exception {
    final String query = "select theta_decode_count(theta(`col_int`)) as col_int, " +
      "theta_decode_count(theta(`col_dt`)) as col_dt, " +
      "theta_decode_count(theta(`col_tmstmp`)) as col_tmstmp, " +
      "theta_decode_count(theta(`col_vrchr`)) as col_vrchr, " +
      "theta_decode_count(theta(`col_tim`)) as col_tim, " +
      "theta_decode_count(theta(`col_flt`)) as col_flt, " +
      "theta_decode_count(theta(`col_chr`)) as col_chr " +
      "from cp.`parquet/alltypes_optional.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(2L, 3L, 3L, 3L, 3L, 3L, 3L)
      .go();
  }

  @Test
  public void testTheta_decode_nullable() throws Exception {
    final String query = "select theta_decode(theta(`col_int`)) as col_int, " +
      "theta_decode(theta(`col_dt`)) as col_dt, " +
      "theta_decode(theta(`col_tmstmp`)) as col_tmstmp, " +
      "theta_decode(theta(`col_vrchr`)) as col_vrchr, " +
      "theta_decode(theta(`col_tim`)) as col_tim, " +
      "theta_decode(theta(`col_flt`)) as col_flt, " +
      "theta_decode(theta(`col_chr`)) as col_chr " +
      "from cp.`parquet/alltypes_optional.parquet`";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int", "col_dt", "col_tmstmp", "col_vrchr", "col_tim", "col_flt", "col_chr")
      .baselineValues(1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
      .go();
  }

  @Test
  public void testTheta_union_decode() throws Exception {
    final String query = "select theta_decode_count(theta_union(s.col_int)) as col_int from (" +
      "  select theta(`col_int`) as col_int from cp.`parquet/alltypes_required.parquet`" +
      "  union all" +
      "  select theta(`col_int`) as col_int from cp.`parquet/alltypes_optional.parquet`" +
      ") s";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int")
      .baselineValues(4L)
      .go();
  }

  @Test
  public void testTheta_intersect_decode() throws Exception {
    final String query = "select theta_decode_count(theta_intersection(s.col_int)) as col_int from (" +
      "  select theta(`col_int`) as col_int from cp.`parquet/alltypes_required.parquet`" +
      "  union all" +
      "  select theta(`col_int`) as col_int from cp.`parquet/alltypes_optional.parquet`" +
      ") s";
    testBuilder().sqlQuery(query).ordered()
      .baselineColumns("col_int")
      .baselineValues(2L)
      .go();
  }
}
