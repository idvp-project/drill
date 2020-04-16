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
<@pp.dropOutputFile />

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/RepeatedUnionFunctions.java" />

<#include "/@includes/license.ftl" />
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
public class RepeatedUnionFunctions {

  private RepeatedUnionFunctions() {
  }

  <#list repeatedTypes.types as type>
  @FunctionTemplate(name = "repeated_union", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedUnion${type.type} implements DrillSimpleFunc {

    @Param ${type.type}Holder input1;
    @Param ${type.type}Holder input2;
    @Output ComplexWriter out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter writer = out.rootAsList();
      writer.startList();

      org.apache.drill.exec.expr.holders.${type.holder} holder = new org.apache.drill.exec.expr.holders.${type.holder}();
      for (int i = input1.start; i < input1.end; i++) {
        input1.vector.getAccessor().get(i, holder);
        <#if type.decimal>
        writer.${type.writer}(input1.reader.getType().getPrecision(), input1.reader.getType().getScale()).write(holder);
        <#else>
        writer.${type.writer}().write(holder);
        </#if>
      }

      for (int i = input2.start; i < input2.end; i++) {
        input2.vector.getAccessor().get(i, holder);
        <#if type.decimal>
        writer.${type.writer}(input1.reader.getType().getPrecision(), input1.reader.getType().getScale()).write(holder);
        <#else>
        writer.${type.writer}().write(holder);
        </#if>
      }

      writer.endList();
    }
  }

  </#list>
}
