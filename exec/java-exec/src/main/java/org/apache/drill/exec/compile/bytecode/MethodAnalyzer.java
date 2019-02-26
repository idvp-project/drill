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

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Analyzer that allows us to inject additional functionality into ASMs basic analysis.
 *
 * <p>We need to be able to keep track of local variables that are assigned to each other
 * so that we can infer their replacability (for scalar replacement). In order to do that,
 * we need to know when local variables are assigned (with the old value being overwritten)
 * so that we can associate them with the new value, and hence determine whether they can
 * also be replaced, or not.
 *
 * <p>In order to capture the assignment operation, we have to provide our own Frame<>, but
 * ASM doesn't provide a direct way to do that. Here, we use the Analyzer's newFrame() methods
 * as factories that will provide our own derivative of Frame<> which we use to detect
 */
public class MethodAnalyzer<V extends Value> extends Analyzer <V> {
  /**
   * Custom Frame<> that captures setLocal() calls in order to associate values
   * that are assigned to the same local variable slot.
   *
   * <p>Since this is almost a pass-through, the constructors' arguments match
   * those from Frame<>.
   */
  private static class AssignmentTrackingFrame<V extends Value> extends Frame<V> {

    private final LocalVariablesView view;

    private AbstractInsnNode currentInsn;

    /**
     * Constructor.
     *
     * @param nLocals the number of locals the frame should have
     * @param nStack the maximum size of the stack the frame should have
     */
    public AssignmentTrackingFrame(final int nLocals,
                                   final int nStack,
                                   final LocalVariablesView view) {
      super(nLocals, nStack);
      this.view = view;
    }

    /**
     * Copy constructor.
     *
     * @param src the frame being copied
     */
    public AssignmentTrackingFrame(final Frame<? extends V> src,
                                   final LocalVariablesView view) {
      super(src);
      this.view = view;
    }

    @Override
    public void execute(final AbstractInsnNode insn,
                        final Interpreter<V> interpreter) throws AnalyzerException {
      currentInsn = insn;
      super.execute(insn, interpreter);
    }

    @Override
    public void setLocal(final int i, final V value) {
      /*
       * If we're replacing one ReplacingBasicValue with another, we need to
       * associate them together so that they will have the same replacability
       * attributes. We also track the local slot the new value will be stored in.
       */
      if (value instanceof ReplacingBasicValue) {
        final ReplacingBasicValue replacingValue = (ReplacingBasicValue) value;
        final LocalVariablesView.Ref ref = view.findRef(currentInsn, i);
        replacingValue.setFrameSlot(i, ref);

        final V localValue = getLocal(i);
        if ((localValue != null) && (localValue instanceof ReplacingBasicValue)) {
          final ReplacingBasicValue localReplacingValue = (ReplacingBasicValue) localValue;

          if (localReplacingValue.containsLocalVariable(ref)) {
            localReplacingValue.associate(replacingValue);

            LoggerFactory.getLogger(MethodAnalyzer.class).error("Current insn: " + currentInsn);
            LoggerFactory.getLogger(MethodAnalyzer.class).error("Variable: " + ref);
            LoggerFactory.getLogger(MethodAnalyzer.class).error("Assigned variables: " + localReplacingValue.frameSlots);

            if (!Objects.equals(((ReplacingBasicValue) value).getIden().type, ((ReplacingBasicValue) localValue).getIden().type)) {
              LoggerFactory.getLogger(MethodAnalyzer.class).error("variables view: " + this.view);


              LoggerFactory.getLogger(MethodAnalyzer.class).error("Type mismatch: " + i);
              LoggerFactory.getLogger(MethodAnalyzer.class).error("new value: " + value);
              LoggerFactory.getLogger(MethodAnalyzer.class).error("old value: " + localValue);
            }

            return;
          }
        }

      }

      super.setLocal(i, value);
    }
  }

  private final LocalVariablesView view;

  /**
   * Constructor.
   *
   * @param interpreter the interpreter to use
   */
  public MethodAnalyzer(final Interpreter<V> interpreter,
                        final MethodNode method) {
    super(interpreter);
    this.view = new LocalVariablesView(method);
  }

  @Override
  protected Frame<V> newFrame(final int maxLocals, final int maxStack) {
    return new AssignmentTrackingFrame<V>(maxLocals, maxStack, view);
  }

  @Override
  protected Frame<V> newFrame(final Frame<? extends V> src) {
    return new AssignmentTrackingFrame<V>(src, view);
  }

  private static final class LocalVariablesView {
    private final MethodNode method;
    private final List<Ref> refs;

    LocalVariablesView(final MethodNode method) {
      this.method = method;
      List<Ref> refs = new ArrayList<>(method.localVariables.size());

      for (final LocalVariableNode lv : method.localVariables) {
        int start = method.instructions.indexOf(lv.start);
        int end = method.instructions.indexOf(lv.end);
        refs.add(new Ref(lv.name, start, end, lv.index));
      }

      refs.sort(Comparator.comparing(r -> r.start));

      this.refs = ImmutableList.copyOf(refs);
    }

    Ref findRef(final AbstractInsnNode insn,
                final int index) {

      if (insn == null) {
        return null;
      }

      int insnIndex = method.instructions.indexOf(insn);

      for (final Ref ref : refs) {
        if (ref.start <= insnIndex && ref.end > insnIndex && ref.index == index) {
          return ref;
        }
      }

      for (final Ref ref : refs) {
        if (ref.start > insnIndex && ref.index == index) {
          // variable creates just after *STORE operation, so try to find next new variable
          return ref;
        }
      }




      LoggerFactory.getLogger(MethodAnalyzer.class).error("variables view: " + this);
      LoggerFactory.getLogger(MethodAnalyzer.class).error("Insn: " + insn);
      LoggerFactory.getLogger(MethodAnalyzer.class).error("Insn code: " + insn.getOpcode());
      LoggerFactory.getLogger(MethodAnalyzer.class).error("Insn index: " + insnIndex);
      LoggerFactory.getLogger(MethodAnalyzer.class).error("Var index: " + index);

      return null;
    }


    @Override
    public String toString() {
      return refs.toString();
    }

    static final class Ref {
      private final String name;
      private final int start;
      private final int end;
      private final int index;

      Ref(final String name,
          final int start,
          final int end,
          final int index) {
        this.name = name;
        this.start = start;
        this.end = end;
        this.index = index;
      }


      @Override
      public String toString() {
        return "Ref{" + "name='" + name + '\'' + ", start=" + start + ", end=" + end + ", index=" + index + '}';
      }
    }
  }
}
