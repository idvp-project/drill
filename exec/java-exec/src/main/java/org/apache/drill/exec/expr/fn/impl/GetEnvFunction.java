package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Param;


/**
 * Add Function for validation pass
 * {@link org.apache.drill.exec.planner.sql.conversion.SqlConverter#rewriteGetEnv}
 */
public class GetEnvFunction {

    public static final String NAME = "get_env";

    @FunctionTemplate(names = {org.apache.drill.exec.expr.fn.impl.GetEnvFunction.NAME}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.INTERNAL)
    public static class GetEnvFunctionImpl {
        @Param
        org.apache.drill.exec.expr.holders.VarCharHolder variable;
        org.apache.drill.exec.expr.holders.VarCharHolder defaultValue;
        @org.apache.drill.exec.expr.annotations.Output
        org.apache.drill.exec.expr.holders.VarCharHolder out;

        public void setup() { }

        public void eval() {
            throw new UnsupportedOperationException("get_env function should rewrite in sql node before executing");
        }
    }
}
