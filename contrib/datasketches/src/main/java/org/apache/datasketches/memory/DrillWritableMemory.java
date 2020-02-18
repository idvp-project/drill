package org.apache.datasketches.memory;

import io.netty.buffer.DrillBuf;

import java.nio.ByteOrder;

/**
 * {@link WritableBuffer} implementation, backed by {@link DrillBuf}.
 * WARNING: This implementation may be broken when DrillBuf behavior changes.
 */
public final class DrillWritableMemory extends WritableMemoryImpl {
  static final int baseTypeId = DIRECT | MEMORY;
  private final int typeId;
  private final MemoryRequestServer memoryRequestServer;

  final DrillBuf buf;

  /**
   * Сan an DrillWritableMemory be closed by DrillMemoryRequestServer.
   * DrillMemoryRequestServer cannot release DrillBuf if it was created externally (e.g. from OperationContext::getManagedBuffer).
   */
  final boolean closeable;

  public static DrillWritableMemory wrap(DrillBuf buf) {
    return new DrillWritableMemory(buf, false);
  }

  public static DrillWritableMemory wrapReadonly(DrillBuf buf) {
    return new DrillWritableMemory(buf, 0, buf.capacity(), DIRECT | MEMORY | NATIVE | READONLY, false);
  }

  DrillWritableMemory(DrillBuf buf, boolean closeable) {
    this(buf, 0, buf.capacity(), baseTypeId | NATIVE, closeable);
  }

  private DrillWritableMemory(DrillBuf buf, long regionOffset, long capacity, int typeId, boolean closeable) {
    super(null, buf.memoryAddress(), regionOffset, capacity);
    this.buf = buf;
    this.typeId = typeId;
    this.closeable = closeable;
    this.memoryRequestServer = new DrillMemoryRequestServer(buf.alloc());
  }

  @Override
  BaseWritableMemoryImpl toWritableRegion(long offsetBytes, long capacityBytes, boolean readOnly, ByteOrder byteOrder) {
    final int type = baseTypeId | REGION | (readOnly ? READONLY : 0) | (isNativeByteOrder(byteOrder) ? NONNATIVE : NATIVE);
    return new DrillWritableMemory(buf, getRegionOffset(offsetBytes), capacityBytes, type, closeable);
  }

  @Override
  BaseWritableBufferImpl toWritableBuffer(boolean readOnly, ByteOrder byteOrder) {
    final int type = baseTypeId | BUFFER
      | (readOnly ? READONLY : 0)
      | (isNativeByteOrder(byteOrder) ? NONNATIVE : NATIVE)
      | (isRegionType() ? REGION : 0);
    return new DrillWritableBuffer(this, type);
  }

  @Override
  long getNativeBaseOffset() {
    return buf.memoryAddress();
  }

  @Override
  int getTypeId() {
    return typeId;
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return memoryRequestServer;
  }
}
