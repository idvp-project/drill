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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/RepeatedContainsFunctions.java" />

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
public class RepeatedContainsFunctions {

  private RepeatedContainsFunctions() {
  }

  <#list repeatedTypes.types as type>
  @FunctionTemplate(name = "repeated_contains", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class RepeatedContains${type.type} implements DrillSimpleFunc {

    @Param ${type.type}Holder input1;
    @Param ${type.type}Holder input2;
    @Output BitHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      <#if type.type == "RepeatedVarChar" || type.type == "RepeatedVarBinary">
      java.util.Set<org.apache.drill.exec.expr.fn.impl.DrillByteArray> collection = new java.util.HashSet<org.apache.drill.exec.expr.fn.impl.DrillByteArray>(input2.end - input2.start);
      <#elseif type.hppcType?has_content>
      com.carrotsearch.hppc.${type.hppcType} collection = new com.carrotsearch.hppc.${type.hppcType}(input2.end - input2.start);
      <#else>
      java.util.Set<Object> collection = new java.util.HashSet<Object>();
      </#if>

      <#if type.type == "RepeatedIntervalDay" || type.type == "RepeatedInterval">
      StringBuilder sb = new StringBuilder();
      </#if>

      org.apache.drill.exec.expr.holders.${type.holder} holder = new org.apache.drill.exec.expr.holders.${type.holder}();
      for (int i = input2.start; i < input2.end; i++) {
        input2.vector.getAccessor().get(i, holder);
        <#if type.decimal>
        java.math.BigDecimal d = input2.vector.getAccessor().getObject(i);
        collection.add(d);
        <#elseif type.type == "RepeatedIntervalDay">
        sb.setLength(0);
        collection.add(sb.append(holder.days).append(holder.milliseconds).toString());
        <#elseif type.type == "RepeatedInterval">
        sb.setLength(0);
        collection.add(sb.append(holder.months).append(holder.days).append(holder.milliseconds).toString());
        <#elseif type.type == "RepeatedVarChar" || type.type == "RepeatedVarBinary">
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes, 0, bytes.length);
        org.apache.drill.exec.expr.fn.impl.DrillByteArray v = new org.apache.drill.exec.expr.fn.impl.DrillByteArray(bytes);
        collection.add(v);
        <#else>
        collection.add(holder.value);
        </#if>
      }

      for (int i = input1.start; i < input1.end; i++) {
        if (collection.isEmpty()) {
          break;
        }

        input1.vector.getAccessor().get(i, holder);
        <#if type.decimal>
        java.math.BigDecimal d = input1.vector.getAccessor().getObject(i);
        collection.remove(d);
        <#elseif type.type == "RepeatedIntervalDay">
        sb.setLength(0);
        collection.remove(sb.append(holder.days).append(holder.milliseconds).toString());
        <#elseif type.type == "RepeatedInterval">
        sb.setLength(0);
        collection.remove(sb.append(holder.months).append(holder.days).append(holder.milliseconds).toString());
        <#elseif type.type == "RepeatedVarChar" || type.type == "RepeatedVarBinary">
        byte[] bytes = new byte[holder.end - holder.start];
        holder.buffer.getBytes(holder.start, bytes, 0, bytes.length);
        org.apache.drill.exec.expr.fn.impl.DrillByteArray v = new org.apache.drill.exec.expr.fn.impl.DrillByteArray(bytes);
        collection.remove(v);
        <#else>
        collection.remove(holder.value);
        </#if>
      }

      if (collection.isEmpty()) {
        out.value = 1;
      } else {
        out.value = 0;
      }
    }
  }

  </#list>
}
