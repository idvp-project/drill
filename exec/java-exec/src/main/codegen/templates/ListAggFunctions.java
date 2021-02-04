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

  <#if type.listagg != true>
    <#continue>
  </#if>

  <#list ["Simple", "WithSeparator", "WithNullableSeparator"] as mode>

  @FunctionTemplate(name = "listagg", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ListAgg${type.source}${mode} implements DrillAggFunc {
    @Param ${type.source}Holder in;
    <#if mode == "WithSeparator">
    @Param VarCharHolder separator;
    <#elseif mode == "WithNullableSeparator">
    @Param NullableVarCharHolder separator;
    </#if>
    @Workspace BitHolder isSet;
    @Workspace ObjectHolder aggregate;
    @Inject DrillBuf buf;
    @Output NullableVarCharHolder out;

    public void setup() {
      isSet = new BitHolder();
      aggregate = new ObjectHolder();
      aggregate.obj = new StringBuilder();
    }

    @Override public void add(){
    <#if type.source?starts_with("Nullable")>
      sout:{
        if(in.isSet == 0){
        // processing nullable input and the value is null, so don't do anything...
        break sout;
        }
    </#if>

      isSet.value = 1;
      StringBuilder sb = (StringBuilder) aggregate.obj;

      <#if !type.type?starts_with("Interval")>
        <#if type.type == "VarLen">
      String str = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(in.start, in.end, in.buffer);
        <#elseif type.type == "VarDecimal">
      String str = org.apache.drill.exec.util.DecimalUtility.getBigDecimalFromDrillBuf(in.buffer, in.start, in.end - in.start, in.scale).toString();
        <#elseif type.type == "Boolean">
      String str = org.apache.drill.common.types.BooleanType.get(String.valueOf(in.value)).name().toLowerCase();
        <#elseif type.type == "Primitive">
      String str = ${type.javaType}.toString(in.value);
        <#elseif type.type == "Time">
      java.time.LocalDateTime temp = java.time.Instant.ofEpochMilli(in.value).atZone(java.time.ZoneOffset.UTC).toLocalDateTime();
      String str = org.apache.drill.exec.expr.fn.impl.DateUtility.format${type.source?replace("Nullable", "")}.format(temp);
        </#if>
      <#if mode == "WithSeparator">
      if (!str.isEmpty() && sb.length() > 0) {
        String s = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(separator.start, separator.end, separator.buffer);
        sb.append(s);
      }
      </#if>
      <#if mode == "WithNullableSeparator">
      if (!str.isEmpty() && sb.length() > 0 && separator.isSet == 1) {
        String s = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(separator.start, separator.end, separator.buffer);
        sb.append(s);
      }
      </#if>
      sb.append(str);
      <#else>
        <#if mode == "WithSeparator">
      if (sb.length() > 0) {
        String s = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(separator.start, separator.end, separator.buffer);
        sb.append(s);
      }
        </#if>
        <#if mode == "WithNullableSeparator">
      if (sb.length() > 0 && separator.isSet == 1) {
        String s = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(separator.start, separator.end, separator.buffer);
        sb.append(s);
      }
        </#if>
      <#if type.type == "IntervalYear">
      int years  = (in.value / org.apache.drill.exec.vector.DateUtilities.yearsToMonths);
      int months = (in.value % org.apache.drill.exec.vector.DateUtilities.yearsToMonths);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";
      sb.append(years).append(yearString).append(months).append(monthString);
      <#elseif type.type == "IntervalDay">
      long millis = in.milliseconds;

      long hours  = millis / (org.apache.drill.exec.vector.DateUtilities.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.vector.DateUtilities.hoursToMillis);

      long minutes = millis / (org.apache.drill.exec.vector.DateUtilities.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.vector.DateUtilities.minutesToMillis);

      long seconds = millis / (org.apache.drill.exec.vector.DateUtilities.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.vector.DateUtilities.secondsToMillis);

      String dayString = (Math.abs(in.days) == 1) ? " day " : " days ";

      sb.append(in.days).append(dayString).
        append(hours).append(":").
        append(minutes).append(":").
        append(seconds).append(".").
        append(millis);
      <#elseif type.type == "Interval">
      int years  = (in.months / org.apache.drill.exec.vector.DateUtilities.yearsToMonths);
      int months = (in.months % org.apache.drill.exec.vector.DateUtilities.yearsToMonths);

      long millis = in.milliseconds;

      long hours  = millis / (org.apache.drill.exec.vector.DateUtilities.hoursToMillis);
      millis     = millis % (org.apache.drill.exec.vector.DateUtilities.hoursToMillis);

      long minutes = millis / (org.apache.drill.exec.vector.DateUtilities.minutesToMillis);
      millis      = millis % (org.apache.drill.exec.vector.DateUtilities.minutesToMillis);

      long seconds = millis / (org.apache.drill.exec.vector.DateUtilities.secondsToMillis);
      millis      = millis % (org.apache.drill.exec.vector.DateUtilities.secondsToMillis);

      String yearString = (Math.abs(years) == 1) ? " year " : " years ";
      String monthString = (Math.abs(months) == 1) ? " month " : " months ";
      String dayString = (Math.abs(in.days) == 1) ? " day " : " days ";


      sb.append(years).append(yearString).
        append(months).append(monthString).
        append(in.days).append(dayString).
        append(hours).append(":").
        append(minutes).append(":").
        append(seconds).append(".").
        append(millis);
      </#if>
    </#if>

  <#if type.source?starts_with("Nullable")>
      }
  </#if>
    }

    @Override public void output() {
      out.isSet = isSet.value;
      if (isSet.value == 1) {
        byte[] result = ((StringBuilder) aggregate.obj).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.buffer = buf = buf.reallocIfNeeded(result.length);
        out.start = 0;
        out.end = result.length;
        out.buffer.setBytes(0, result);
      }
    }

    @Override public void reset() {
      isSet.value = 0;
      ((StringBuilder) aggregate.obj).setLength(0);
    }
  }


  </#list>
  </#list>

}

