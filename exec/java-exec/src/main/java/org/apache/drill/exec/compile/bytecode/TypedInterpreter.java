package org.apache.drill.exec.compile.bytecode;

import org.apache.drill.exec.compile.CompilationConfig;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Interpreter;
import org.objectweb.asm.tree.analysis.SourceInterpreter;
import org.objectweb.asm.tree.analysis.SourceValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author ozinoviev
 * @since 07.11.18
 */
public class TypedInterpreter extends Interpreter<TypedValue> implements Opcodes {

  private final BasicInterpreter basicInterpreter;
  private final SourceInterpreter sourceInterpreter;

  public TypedInterpreter() {
    super(CompilationConfig.ASM_API_VERSION);
    basicInterpreter = new BasicInterpreter();
    sourceInterpreter = new SourceInterpreter();
  }


  @Override
  public TypedValue newValue(Type type) {
    BasicValue basicValue = basicInterpreter.newValue(type);
    SourceValue sourceValue = sourceInterpreter.newValue(type);
    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue newOperation(AbstractInsnNode insn) throws AnalyzerException {
    BasicValue basicValue = basicInterpreter.newOperation(insn);
    SourceValue sourceValue = sourceInterpreter.newOperation(insn);

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue copyOperation(AbstractInsnNode insn, TypedValue value) throws AnalyzerException {
    BasicValue basicValue = basicInterpreter.copyOperation(insn, TypedValue.toBasicValue(value));
    SourceValue sourceValue = sourceInterpreter.copyOperation(insn, TypedValue.toSourceValue(value));

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue unaryOperation(AbstractInsnNode insn, TypedValue value) throws AnalyzerException {
    BasicValue basicValue = basicInterpreter.unaryOperation(insn, TypedValue.toBasicValue(value));
    SourceValue sourceValue = sourceInterpreter.unaryOperation(insn, TypedValue.toSourceValue(value));

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue binaryOperation(AbstractInsnNode insn, TypedValue value1, TypedValue value2) throws AnalyzerException {
    BasicValue basicValue = basicInterpreter.binaryOperation(insn, TypedValue.toBasicValue(value1), TypedValue.toBasicValue(value2));
    SourceValue sourceValue = sourceInterpreter.binaryOperation(insn, TypedValue.toSourceValue(value1), TypedValue.toSourceValue(value2));

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue ternaryOperation(AbstractInsnNode insn, TypedValue value1, TypedValue value2, TypedValue value3) throws AnalyzerException {
    BasicValue basicValue = basicInterpreter.ternaryOperation(insn, TypedValue.toBasicValue(value1), TypedValue.toBasicValue(value2), TypedValue.toBasicValue(value3));
    SourceValue sourceValue = sourceInterpreter.ternaryOperation(insn, TypedValue.toSourceValue(value1), TypedValue.toSourceValue(value2), TypedValue.toSourceValue(value3));

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public TypedValue naryOperation(AbstractInsnNode insn, List<? extends TypedValue> values) throws AnalyzerException {
    List<BasicValue> basicValues = new ArrayList<>();
    List<SourceValue> sourceValues = new ArrayList<>();

    // ??
    if (values != null) {
      for (TypedValue value : values) {
        basicValues.add(TypedValue.toBasicValue(value));
        sourceValues.add(TypedValue.toSourceValue(value));
      }
    }

    BasicValue basicValue = basicInterpreter.naryOperation(insn, basicValues);
    SourceValue sourceValue = sourceInterpreter.naryOperation(insn, sourceValues);

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }

  @Override
  public void returnOperation(AbstractInsnNode insn, TypedValue value, TypedValue expected) throws AnalyzerException {
    basicInterpreter.returnOperation(insn, TypedValue.toBasicValue(value), TypedValue.toBasicValue(expected));
    sourceInterpreter.returnOperation(insn, TypedValue.toSourceValue(value), TypedValue.toSourceValue(expected));
  }

  @Override
  public TypedValue merge(TypedValue value1, TypedValue value2) {
    if (Objects.equals(value1, value2)) {
      return value1;
    }

    BasicValue basicValue = basicInterpreter.merge(TypedValue.toBasicValue(value1), TypedValue.toBasicValue(value2));
    SourceValue sourceValue = sourceInterpreter.merge(TypedValue.toSourceValue(value1), TypedValue.toSourceValue(value2));

    if (sourceValue == null) {
      return null;
    }

    return new TypedValue(basicValue, sourceValue);
  }
}
