package org.apache.datasketches.memory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;

/**
 * {@link MemoryRequestServer}, backend by Drill BufferAllocator.
 */
final class _DrillMemoryRequestServer0 implements MemoryRequestServer {
  private final ByteBufAllocator allocator;

  _DrillMemoryRequestServer0(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public WritableMemory request(long capacityBytes) {
    DrillBuf buffer = (DrillBuf) allocator.directBuffer((int) capacityBytes);
    // todo: remove after https://jira.apache.org/jira/browse/DRILL-5530 fix
    buffer.setZero(0, buffer.capacity());
    return new _DrillMemory0(buffer, true);
  }

  @Override
  public void requestClose(WritableMemory memToClose, WritableMemory newMemory) {
    if (memToClose instanceof _DrillMemory0) {
      if (((_DrillMemory0) memToClose).closeable) {
        ((_DrillMemory0) memToClose).getDrillBuf().release();
      }
    }
  }
}
