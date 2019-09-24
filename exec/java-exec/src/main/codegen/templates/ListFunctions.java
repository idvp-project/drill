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

<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/ListFunctions.java" />

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
import org.apache.drill.exec.vector.complex.writer.BaseWriter.*;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;

@SuppressWarnings("unused")
public class ListFunctions {
  <#list listagg.types as type>
  @FunctionTemplate(name = "list", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class List${type.source} implements DrillAggFunc {
    @Param ${type.source}Holder in;
    @Inject DrillBuf buf;
    @Workspace BitHolder initialized;
    @Output ComplexWriter writer;

    public void setup() {
      initialized = new BitHolder();
      initialized.value = 0;
    }

    @Override public void add(){
      if (initialized.value == 0) {
        writer.rootAsList().startList();
      <#if ((type.javaType)!"") == "Integer">
        writer.rootAsList().integer();
      <#elseif type.type == "VarDecimal">
        writer.rootAsList().varDecimal(in.precision, in.scale);
      <#else>
        writer.rootAsList().${type.source?replace("Nullable", "")?uncap_first}();
      </#if>
        initialized.value = 1;
      }

      <#if type.source?starts_with("Nullable")>
      sout:{
        if(in.isSet == 0){
        // processing nullable input and the value is null, so don't do anything...
        break sout;
        }
      </#if>

      <#if type.type == "VarLen">
      writer.rootAsList().${type.source?replace("Nullable", "")?uncap_first}().write${type.source?replace("Nullable", "")?cap_first}(in.start, in.end, in.buffer);
      <#elseif type.type == "VarDecimal">
      writer.rootAsList().varDecimal(in.precision, in.scale).writeVarDecimal(in.start, in.end, in.buffer, in.precision, in.scale);
      <#elseif type.type == "Primitive" || type.type == "Boolean" || type.type == "Time">
        <#if ((type.javaType)!"") == "Integer">
      writer.rootAsList().integer().writeInt(in.value);
        <#else>
      writer.rootAsList().${type.source?replace("Nullable", "")?uncap_first}().write${type.source?replace("Nullable", "")?cap_first}(in.value);
        </#if>
      <#elseif type.type == "Interval">
      writer.rootAsList().interval().writeInterval(in.months, in.days, in.milliseconds);
      <#elseif type.type == "IntervalDay">
      writer.rootAsList().intervalDay().writeIntervalDay(in.days, in.milliseconds);
      <#elseif type.type == "IntervalYear">
      writer.rootAsList().intervalYear().writeIntervalYear(in.value);
      </#if>


  <#if type.source?starts_with("Nullable")>
      }
  </#if>
    }

    @Override public void output() {
      //Do nothing since the complex writer takes care of everything!
    }

    @Override public void reset() {
      initialized.value = 0;
    }
  }


  </#list>

}

