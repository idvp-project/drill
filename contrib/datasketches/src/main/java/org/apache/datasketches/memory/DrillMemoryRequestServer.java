package org.apache.datasketches.memory;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.DrillBuf;

/**
 * {@link MemoryRequestServer}, backend by Drill BufferAllocator.
 */
final class DrillMemoryRequestServer implements MemoryRequestServer {
  private final ByteBufAllocator allocator;

  DrillMemoryRequestServer(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public WritableMemory request(long capacityBytes) {
    DrillBuf buffer = (DrillBuf) allocator.directBuffer((int) capacityBytes);
    // todo: remove after https://jira.apache.org/jira/browse/DRILL-5530 fix
    buffer.setZero(0, buffer.capacity());
    return new DrillWritableMemory(buffer, true);
  }

  @Override
  public void requestClose(WritableMemory memToClose, WritableMemory newMemory) {
    if (memToClose instanceof DrillWritableMemory) {
      if (((DrillWritableMemory) memToClose).closeable) {
        ((DrillWritableMemory) memToClose).buf.release();
      }
    }
  }
}
