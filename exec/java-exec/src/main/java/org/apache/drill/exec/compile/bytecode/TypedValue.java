package org.apache.drill.exec.compile.bytecode;

import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.SourceValue;
import org.objectweb.asm.tree.analysis.Value;

import java.util.Objects;
import java.util.Set;

public class TypedValue implements Value {
  private final BasicValue basicValue;
  private final SourceValue sourceValue;

  TypedValue(final TypedValue other) {
    this.basicValue = other.basicValue;
    this.sourceValue = other.sourceValue;
  }

  TypedValue(final BasicValue basicValue,
             final SourceValue sourceValue) {
    this.basicValue = basicValue;
    this.sourceValue = sourceValue;

    assert this.sourceValue != null;
    assert this.basicValue == null || this.basicValue.getSize() == this.sourceValue.getSize();
  }


  public Type getType() {
    if (basicValue == null) {
      return null;
    }

    return basicValue.getType();
  }

  public boolean isReference() {
    return basicValue != null && basicValue.isReference();
  }

  public Set<AbstractInsnNode> getSources() {
    return sourceValue.insns;
  }

  @Override
  public int getSize() {
    return sourceValue.getSize();
  }

  public static BasicValue toBasicValue(TypedValue typedValue) {
    if (typedValue == null) {
      return null;
    }

    return typedValue.basicValue;
  }

  public static SourceValue toSourceValue(TypedValue typedValue) {
    if (typedValue == null) {
      return null;
    }

    return typedValue.sourceValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof TypedValue)) {
      return false;
    }

    TypedValue that = (TypedValue) o;
    return Objects.equals(basicValue, that.basicValue)
      && Objects.equals(sourceValue, that.sourceValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(basicValue, sourceValue);
  }

  @Override
  public String toString() {
    if (basicValue == null) {
      return "";
    }

    return basicValue.toString();
  }
}
