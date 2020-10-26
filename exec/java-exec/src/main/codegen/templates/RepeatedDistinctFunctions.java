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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/RepeatedDistinctFunctions.java" />

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
public class RepeatedDistinctFunctions {

  private RepeatedDistinctFunctions() {
  }

  <#list repeatedTypes.types as type>
  @FunctionTemplate(name = "repeated_distinct", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedDistinct${type.type} implements DrillSimpleFunc {

    @Param ${type.type}Holder input;
    @Output ComplexWriter out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      <#if type.type == "RepeatedVarChar" || type.type == "RepeatedVarBinary">
      java.util.Set<org.apache.drill.exec.expr.fn.impl.DrillByteArray> collection = new java.util.HashSet<org.apache.drill.exec.expr.fn.impl.DrillByteArray>(input.end - input.start);
      <#elseif type.hppcType?has_content>
      com.carrotsearch.hppc.${type.hppcType} collection = new com.carrotsearch.hppc.${type.hppcType}(input.end - input.start);
      <#else>
      java.util.Set<Object> collection = new java.util.HashSet<Object>();
      </#if>

      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter writer = out.rootAsList();
      writer.startList();

      <#if type.type == "RepeatedIntervalDay" || type.type == "RepeatedInterval">
      StringBuilder sb = new StringBuilder();
      </#if>

      boolean empty = true;
      org.apache.drill.exec.expr.holders.${type.holder} holder = new org.apache.drill.exec.expr.holders.${type.holder}();
      for (int i = input.start; i < input.end; i++) {
        input.vector.getAccessor().get(i, holder);
        <#if type.decimal>
        java.math.BigDecimal d = input.vector.getAccessor().getObject(i);
        if (collection.add(d)) {
          empty = false;
          writer.${type.writer}(input.reader.getType().getPrecision(), input.reader.getType().getScale()).write(holder);
        }
        <#elseif type.type == "RepeatedIntervalDay">
        sb.setLength(0);
        if (collection.add(sb.append(holder.days).append(holder.milliseconds).toString())) {
          empty = false;
          writer.${type.writer}().write(holder);
        }
        <#elseif type.type == "RepeatedInterval">
        sb.setLength(0);
        if (collection.add(sb.append(holder.months).append(holder.days).append(holder.milliseconds).toString())) {
          empty = false;
          writer.${type.writer}().write(holder);
        }
        <#elseif type.type == "RepeatedVarChar" || type.type == "RepeatedVarBinary">
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes, 0, bytes.length);
        org.apache.drill.exec.expr.fn.impl.DrillByteArray v = new org.apache.drill.exec.expr.fn.impl.DrillByteArray(bytes);
        if (collection.add(v)) {
          empty = false;
          writer.${type.writer}().write(holder);
        }
        <#else>
        if (collection.add(holder.value)) {
          empty = false;
          writer.${type.writer}().write(holder);
        }
        </#if>
      }

      if (empty) {
        <#if type.decimal>
        writer.${type.writer}(input.reader.getType().getPrecision(), input.reader.getType().getScale());
        <#else>
        writer.${type.writer}();
        </#if>
      }

      writer.endList();
    }
  }

  </#list>
}
