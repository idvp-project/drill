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

<@pp.changeOutputFile name="/org/apache/drill/exec/udfs/datasketches/ThetaFunctions.java" />

<#include "/@includes/license.ftl" />

<#assign func = theta>

package org.apache.drill.exec.udfs.datasketches;


import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.impl.StringFunctions;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;
import io.netty.buffer.DrillBuf;

import javax.inject.Inject;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class ThetaFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${func.className}Functions.class);

  <#list func.types as type>
  @FunctionTemplate(name = "${func.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ${type.inputType}${func.className} implements DrillAggFunc {
    @Param ${type.inputType}Holder in;
    @Inject DrillBuf workBuf;
    @Inject DrillBuf outputBuf;
    @Output VarBinaryHolder out;
    @Workspace ObjectHolder sketchHolder;

    @Override
    public void setup() {
      workBuf = workBuf.reallocIfNeeded(org.apache.datasketches.theta.Sketch.getMaxUpdateSketchBytes(org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES));
      org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrap(workBuf);
      org.apache.datasketches.theta.UpdateSketch sketch = org.apache.datasketches.theta.UpdateSketch.builder()
        .setMemoryRequestServer(memory.getMemoryRequestServer())
        .setFamily(org.apache.datasketches.Family.QUICKSELECT)
        .build(memory);
      sketchHolder = new ObjectHolder();
      sketchHolder.obj = sketch;
    }

    @Override
    public void add() {
      <#if type.inputType?starts_with("Nullable")>
      sout: {
        if (in.isSet == 0) {
          // processing nullable input and the value is null, so don't do anything...
          break sout;
        }
      </#if>

        org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
        <#if type.type == "simple">
        sketch.update(in.value);
        </#if>

        <#if type.type == "varlen">
        byte[] data = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, data, 0, data.length);
        sketch.update(data);
        </#if>

        <#if type.type == "vardecimal">
        java.math.BigDecimal value = org.apache.drill.exec.util.DecimalUtility
          .getBigDecimalFromDrillBuf(in.buffer, in.start, in.end - in.start, in.scale);
        sketch.update(value.toString());
        </#if>
      <#if type.inputType?starts_with("Nullable")>
      }
      </#if>
    }

    @Override
    public void output() {
      org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
      byte[] result = sketch.compact().toByteArray();
      outputBuf = outputBuf.reallocIfNeeded(result.length);
      outputBuf.setBytes(0, result, 0, result.length);
      out.start = 0;
      out.end = result.length;
      out.buffer = outputBuf;
    }

    @Override
    public void reset() {
      org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
      sketch.reset();
    }
  }

  </#list>

  <#list func.types as type>
  @FunctionTemplate(name = "${func.funcName}_count", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ${type.inputType}${func.className}Count implements DrillAggFunc {
    @Param ${type.inputType}Holder in;
    @Inject DrillBuf workBuf;
    @Output BigIntHolder out;
    @Workspace ObjectHolder sketchHolder;

    @Override
    public void setup() {
      workBuf = workBuf.reallocIfNeeded(org.apache.datasketches.theta.Sketch.getMaxUpdateSketchBytes(org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES));
      org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrap(workBuf);
      org.apache.datasketches.theta.UpdateSketch sketch = org.apache.datasketches.theta.UpdateSketch.builder()
        .setMemoryRequestServer(memory.getMemoryRequestServer())
        .setFamily(org.apache.datasketches.Family.QUICKSELECT)
        .build(memory);
      sketchHolder = new ObjectHolder();
      sketchHolder.obj = sketch;
    }

    @Override
    public void add() {
      <#if type.inputType?starts_with("Nullable")>
      sout: {
        if (in.isSet == 0) {
          // processing nullable input and the value is null, so don't do anything...
          break sout;
        }
      </#if>
        org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
        <#if type.type == "simple">
        sketch.update(in.value);
        </#if>

        <#if type.type == "varlen">
        byte[] data = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, data, 0, data.length);
        sketch.update(data);
        </#if>

        <#if type.type == "vardecimal">
        java.math.BigDecimal value = org.apache.drill.exec.util.DecimalUtility
          .getBigDecimalFromDrillBuf(in.buffer, in.start, in.end - in.start, in.scale);
        sketch.update(value.toString());
        </#if>
      <#if type.inputType?starts_with("Nullable")>
      }
      </#if>
    }

    @Override
    public void output() {
      org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
      out.value = (long) sketch.getEstimate();
    }

    @Override
    public void reset() {
      org.apache.datasketches.theta.UpdateSketch sketch = (org.apache.datasketches.theta.UpdateSketch) sketchHolder.obj;
      sketch.reset();
    }
  }
  </#list>

  <#list func.merge.funcNames as mergeFunc>
    <#list func.merge.types as type>
  @FunctionTemplate(name = "${func.funcName}_${mergeFunc}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class ${type}${func.className}${mergeFunc} implements DrillAggFunc {
    @Param ${type}Holder in;
    @Inject DrillBuf workBuf;
    @Inject DrillBuf outputBuf;
    @Output VarBinaryHolder out;
    @Workspace ObjectHolder operationHolder;

    @Override
    public void setup() {
      workBuf = workBuf.reallocIfNeeded(org.apache.datasketches.theta.Sketch.getMaxUpdateSketchBytes(org.apache.datasketches.Util.DEFAULT_NOMINAL_ENTRIES));
      org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrap(workBuf);
      org.apache.datasketches.theta.${mergeFunc?capitalize} setOperation = org.apache.datasketches.theta.SetOperation.builder()
        .setMemoryRequestServer(memory.getMemoryRequestServer())
        .build${mergeFunc?capitalize}(memory);
      operationHolder = new ObjectHolder();
      operationHolder.obj = setOperation;
    }

     @Override
    public void add() {
      <#if type?starts_with("Nullable")>
      sout: {
        if (in.isSet == 0) {
          // processing nullable input and the value is null, so don't do anything...
          break sout;
        }
      </#if>
        org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrapReadonly(in.buffer.slice(in.start, in.end - in.start));
        org.apache.datasketches.theta.Sketch sketch = org.apache.datasketches.theta.Sketches.wrapSketch(memory);
        org.apache.datasketches.theta.${mergeFunc?capitalize} setOperation = (org.apache.datasketches.theta.${mergeFunc?capitalize}) operationHolder.obj;
        setOperation.update(sketch);
      <#if type?starts_with("Nullable")>
      }
      </#if>
    }

    @Override
    public void output() {
      org.apache.datasketches.theta.${mergeFunc?capitalize} setOperation = (org.apache.datasketches.theta.${mergeFunc?capitalize}) operationHolder.obj;
      byte[] result = setOperation.getResult().toByteArray();
      outputBuf = outputBuf.reallocIfNeeded(result.length);
      outputBuf.setBytes(0, result, 0, result.length);
      out.start = 0;
      out.end = result.length;
      out.buffer = outputBuf;
    }

    @Override
    public void reset() {
      org.apache.datasketches.theta.${mergeFunc?capitalize} setOperation = (org.apache.datasketches.theta.${mergeFunc?capitalize}) operationHolder.obj;
      setOperation.reset();
    }
  }

    </#list>
  </#list>

  @FunctionTemplate(name = "${func.funcName}_decode", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}Decode implements DrillSimpleFunc {
    @Param VarBinaryHolder in;
    @Output Float8Holder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrapReadonly(in.buffer.slice(in.start, in.end - in.start));
      org.apache.datasketches.theta.Sketch sketch = org.apache.datasketches.theta.Sketches.wrapSketch(memory);
      out.value = sketch.getTheta();
    }
  }

  @FunctionTemplate(name = "${func.funcName}_decode_count", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ${func.className}DecodeCount implements DrillSimpleFunc {
    @Param VarBinaryHolder in;
    @Output BigIntHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      org.apache.datasketches.memory.DrillWritableMemory memory = org.apache.datasketches.memory.DrillWritableMemory.wrapReadonly(in.buffer.slice(in.start, in.end - in.start));
      org.apache.datasketches.theta.Sketch sketch = org.apache.datasketches.theta.Sketches.wrapSketch(memory);
      out.value = (long) sketch.getEstimate();
    }
  }
}
