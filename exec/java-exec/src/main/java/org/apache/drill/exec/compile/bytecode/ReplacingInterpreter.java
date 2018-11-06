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
package org.apache.drill.exec.compile.bytecode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.drill.exec.compile.CompilationConfig;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.analysis.AnalyzerException;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.objectweb.asm.tree.analysis.Interpreter;
import org.objectweb.asm.tree.analysis.Value;

public class ReplacingInterpreter extends Interpreter<Value> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReplacingInterpreter.class);

  private final String className; // fully qualified internal class name
  private int index = 0;
  private final List<ReplacingValue> valueList;

  private final TypedInterpreter interpreter;

  public ReplacingInterpreter(final String className, final List<ReplacingValue> valueList) {
    super(CompilationConfig.ASM_API_VERSION);
    this.className = className;
    this.valueList = valueList;
    this.interpreter = new TypedInterpreter();
  }

  private static String desc(Class<?> c) {
    final Type t = Type.getType(c);
    return t.getDescriptor();
  }

  @Override
  public Value newValue(Type type) {
    TypedValue value = interpreter.newValue(type);

    if (type != null) {
      final ValueHolderIden iden = HOLDERS.get(type.getDescriptor());
      if (iden != null) {
        final ReplacingValue v = ReplacingValue.create(value, iden, index++, valueList);
        v.markFunctionReturn();
        return v;
      }

      // We need to track use of the "this" objectref
      if ((type.getSort() == Type.OBJECT) && className.equals(type.getInternalName())) {
        final ReplacingValue rbValue = ReplacingValue.create(value, null, 0, valueList);
        rbValue.setThis();
        return rbValue;
      }
    }

    return value;
  }

  @Override
  public Value newOperation(AbstractInsnNode insn) throws AnalyzerException {
    TypedValue value = interpreter.newOperation(insn);

    if (insn.getOpcode() == Opcodes.NEW) {
      final TypeInsnNode t = (TypeInsnNode) insn;

      // if this is for a holder class, we'll replace it
      final ValueHolderIden iden = HOLDERS.get(t.desc);
      if (iden != null) {
        return ReplacingValue.create(value, iden, index++, valueList);
      }
    }

    return value;
  }

  @Override
  public Value copyOperation(AbstractInsnNode insn, Value value) throws AnalyzerException {
    TypedValue tv = unwrap(value);
    tv = interpreter.copyOperation(insn, tv);
    return mergeWith(value, tv);
  }

  @Override
  public Value unaryOperation(AbstractInsnNode insn, Value value) throws AnalyzerException {
    /*
     * We're looking for the assignment of an operator member variable that's a holder to a local
     * objectref. If we spot that, we can't replace the local objectref (at least not
     * until we do the work to replace member variable holders).
     *
     * Note that a GETFIELD does not call newValue(), as would happen for a local variable, so we're
     * emulating that here.
     */
    if ((insn.getOpcode() == Opcodes.GETFIELD) && (value instanceof ReplacingValue)) {
      final ReplacingValue possibleThis = (ReplacingValue) value;
      if (possibleThis.isThis()) {
        final FieldInsnNode fieldInsn = (FieldInsnNode) insn;
        if (HOLDERS.get(fieldInsn.desc) != null) {
          final TypedValue fetchedField = interpreter.unaryOperation(insn, unwrap(value));
          final ReplacingValue replacingValue = ReplacingValue.create(fetchedField, null, -1, valueList);
          replacingValue.setAssignedToMember();
          return replacingValue;
        }
      }
    }

    return interpreter.unaryOperation(insn,  unwrap(value));
  }

  @Override
  public Value binaryOperation(AbstractInsnNode insn, Value value1, Value value2) throws AnalyzerException {
    final TypedValue tv2 = unwrap(value2);

    /*
     * We're looking for the assignment of a local holder objectref to a member variable.
     * If we spot that, then the local holder can't be replaced, since we don't (yet)
     * have the mechanics to replace the member variable with the holder's members or
     * to assign all of them when this happens.
     */
    if (insn.getOpcode() == Opcodes.PUTFIELD) {
      if (tv2.isReference() && (value1 instanceof ReplacingValue)) {
        final ReplacingValue possibleThis = (ReplacingValue) value1;
        if (possibleThis.isThis() && (value2 instanceof ReplacingValue)) {
          // if this is a reference for a holder class, we can't replace it
          if (HOLDERS.get(((ReplacingValue) value2).getTypedValue().getType().getDescriptor()) != null) {
            final ReplacingValue localRef = (ReplacingValue) value2;
            localRef.setAssignedToMember();
          }
        }
      }
    }

    return interpreter.binaryOperation(insn, unwrap(value1), tv2);
  }

  @Override
  public Value ternaryOperation(AbstractInsnNode insn, Value value1, Value value2, Value value3) throws AnalyzerException {
    return interpreter.ternaryOperation(insn, unwrap(value1), unwrap(value2), unwrap(value3));
  }

  @Override
  public Value naryOperation(AbstractInsnNode insn, List<? extends Value> values) throws AnalyzerException {
    if (insn instanceof MethodInsnNode) {
      boolean skipOne = insn.getOpcode() != Opcodes.INVOKESTATIC;

      // Note if the argument is a holder, and is used as a function argument
      for(Value value : values) {
        // if non-static method, skip over the receiver
        if (skipOne) {
          skipOne = false;
          continue;
        }

        if (value instanceof ReplacingValue) {
          final ReplacingValue argument = (ReplacingValue) value;
          argument.setFunctionArgument();
        }
      }
    }

    List<TypedValue> typedValues = new ArrayList<>(values.size());
    for (Value value : values) {
      typedValues.add(unwrap(value));
    }

    return interpreter.naryOperation(insn,  typedValues);
  }

  @Override
  public void returnOperation(AbstractInsnNode insn, Value value, Value expected) throws AnalyzerException {
    interpreter.returnOperation(insn, unwrap(value), unwrap(expected));
  }

  @Override
  public Value merge(Value value1, Value value2) {
    if (Objects.equals(value1, value2)) {
      return value1;
    }

    TypedValue tv1 = unwrap(value1);
    TypedValue tv2 = unwrap(value2);

    TypedValue merged = interpreter.merge(tv1, tv2);

    if (value1 instanceof ReplacingValue) {
      return mergeWith(value1, merged);
    }

    return mergeWith(value2, merged);
  }

  private TypedValue unwrap(Value value) {
    TypedValue tv;

    if (value instanceof ReplacingValue) {
      ReplacingValue rv = (ReplacingValue) value;
      tv = rv.getTypedValue();
    } else {
      tv = (TypedValue) value;
    }
    return tv;
  }

  private Value mergeWith(Value sourceValue, TypedValue typedValue) {
    if (sourceValue instanceof ReplacingValue) {
      ReplacingValue rv = (ReplacingValue) sourceValue;
      ReplacingValue newValue = ReplacingValue.create(typedValue, rv.getIden(), rv.getIndex(), valueList);
      rv.associate(newValue);
      return newValue;
    }

    return typedValue;
  }

  static {
    ImmutableMap.Builder<String, ValueHolderIden> builder = ImmutableMap.builder();
    ImmutableSet.Builder<String> setB = ImmutableSet.builder();
    for (Class<?> c : ScalarReplacementTypes.CLASSES) {
      String desc = desc(c);
      setB.add(desc);
      String desc2 = desc.substring(1, desc.length() - 1);
      ValueHolderIden vhi = new ValueHolderIden(c);
      builder.put(desc, vhi);
      builder.put(desc2, vhi);
    }
    HOLDER_DESCRIPTORS = setB.build();
    HOLDERS = builder.build();
  }

  private final static ImmutableMap<String, ValueHolderIden> HOLDERS;
  public final static ImmutableSet<String> HOLDER_DESCRIPTORS;
}
