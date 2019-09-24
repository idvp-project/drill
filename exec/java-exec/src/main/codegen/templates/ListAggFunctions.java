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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/ListAggFunctions.java" />

<#include "/@includes/license.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

package org.apache.drill.exec.expr.fn.impl.gaggr;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.*;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;

@SuppressWarnings("unused")
public class ListAggFunctions {
  <#list listagg.types as type>
  <#list ["simple", "with_separator"] as mode>

  @FunctionTemplate(name = "listagg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ListAgg${type.source}<#if mode == "with_separator">WithSeparator</#if> implements DrillAggFunc {
    @Param ${type.source}Holder in;
    <#if mode == "with_separator">
    @Param VarCharHolder inSep;
    </#if>

    @Workspace NullableVarCharHolder aggregate;

    @Inject DrillBuf buf;
    @Output NullableVarCharHolder out;

    public void setup() {
      aggregate = new NullableVarCharHolder();
      aggregate.buffer = buf;
    }

    @Override public void add(){
    <#if type.source?starts_with("Nullable")>
      sout:{
        if(in.isSet == 0){
        // processing nullable input and the value is null, so don't do anything...
        break sout;
        }
    </#if>

      byte[] input;
      <#if type.type == "VarLen">
      input = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, input, input.length, input.length);
      <#elseif type.type == "Fixed">
        <#if type.javaType??>
      input = ${type.javaType}.toString(in.value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        </#if>
      </#if>
      if (input.length > 0) {
        aggregate.isSet = 1;

        DrillBuf previous = aggregate.buffer.duplicate();

        <#if mode == "with_separator">
        byte[] separator = new byte[inSep.end - inSep.start];
        inSep.buffer.getBytes(inSep.start, separator, separator.length, separator.length);
        aggregate.buffer = buf = buf.reallocIfNeeded(aggregate.end - aggregate.start + input.length + separator.length);
        <#else>
        aggregate.buffer = buf = buf.reallocIfNeeded(aggregate.end - aggregate.start + input.length);
        </#if>
        aggregate.buffer.setBytes(aggregate.start, previous, aggregate.start, aggregate.end - aggregate.start);
        aggregate.buffer.setBytes(aggregate.end, input, 0, input.length);
        aggregate.end += input.length;

        <#if mode == "with_separator">
        aggregate.buffer.setBytes(aggregate.end, separator, 0, separator.length);
        aggregate.end += separator.length;
        </#if>

        previous.release();
      }

  <#if type.source?starts_with("Nullable")>
      }
  </#if>
    }

    @Override public void output() {
      out.isSet = aggregate.isSet;
      if (aggregate.isSet == 1) {
        out.buffer = aggregate.buffer;
        out.start = aggregate.start;
        out.end = aggregate.end;
      }
    }

    @Override public void reset() {
      aggregate.isSet = 0;
      aggregate.start = aggregate.end = 0;
      aggregate.buffer = null;
    }
  }
  </#list>
  </#list>

}

