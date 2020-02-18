package org.apache.datasketches.memory;

import java.nio.ByteOrder;

/**
 * {@link WritableBuffer} implementation, backed by {@link DrillWritableMemory}.
 * WARNING: This implementation may be broken when DrillBuf behavior changes.
 */
final class DrillWritableBuffer extends WritableBufferImpl {
  private final int typeId;

  DrillWritableBuffer(DrillWritableMemory originMemory, int typeId) {
    super(null, originMemory.getNativeBaseOffset(), originMemory.getRegionOffset(), originMemory.getCapacity(), originMemory);
    this.typeId = typeId;
  }

  private DrillWritableBuffer(DrillWritableMemory originMemory, int typeId, long offsetBytes, long capacityBytes) {
    super(null, originMemory.getNativeBaseOffset(), offsetBytes, capacityBytes, originMemory);
    this.typeId = typeId;
  }

  @Override
  BaseWritableBufferImpl toWritableRegion(long offsetBytes, long capacityBytes, boolean readOnly, ByteOrder byteOrder) {
    final int type = DrillWritableMemory.baseTypeId | REGION | BUFFER
      | (readOnly ? READONLY : 0)
      | (isNativeByteOrder(byteOrder) ? NONNATIVE : NATIVE)
      | (isDuplicateType() ? DUPLICATE : 0);
    return new DrillWritableBuffer((DrillWritableMemory) originMemory, type, getRegionOffset(offsetBytes), capacityBytes);
  }

  @Override
  BaseWritableBufferImpl toDuplicate(boolean readOnly, ByteOrder byteOrder) {
    final int type = DrillWritableMemory.baseTypeId | BUFFER
      | (readOnly ? READONLY : 0)
      | (isNativeByteOrder(byteOrder) ? NONNATIVE : NATIVE)
      | (isRegionType() ? REGION : 0);
    return new DrillWritableBuffer((DrillWritableMemory) originMemory, type);
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return originMemory.getMemoryRequestServer();
  }

  @Override
  int getTypeId() {
    return typeId;
  }
}
