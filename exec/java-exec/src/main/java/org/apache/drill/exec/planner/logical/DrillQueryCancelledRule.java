package org.apache.drill.exec.planner.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.ops.QueryCancelledException;

/**
 * Rule allows interrupt the planning phase when the query is cancelled.
 **/
public class DrillQueryCancelledRule extends RelOptRule {

  public static final DrillQueryCancelledRule INSTANCE = new DrillQueryCancelledRule();

  private DrillQueryCancelledRule() {
    super(RelOptHelper.any(RelNode.class));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    checkInterrupted();
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    throw new UnsupportedOperationException();
  }

  private void checkInterrupted() {
    if (Thread.interrupted()) {
      throw new QueryCancelledException();
    }
  }
}
