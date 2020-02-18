package org.apache.datasketches.memory;

import java.nio.ByteOrder;

/**
 * Alternative implementation of {@link DrillWritableBuffer}.
 * Unlike {@link DrillWritableMemory}, {@link _DrillMemory0} is not based on the standard implementation
 * of datasketckes {@link WritableBufferImpl}. Instead it delegates memory access method call to appropriate {@link _DrillMemory0} method.
 */
final class _DrillMemoryBuffer0 extends WritableBuffer {

  private final _DrillMemory0 memory;

  private final int typeId;

  _DrillMemoryBuffer0(_DrillMemory0 memory) {
    this(memory, memory.getTypeId() | BUFFER);
  }

  private _DrillMemoryBuffer0(_DrillMemory0 memory, int typeId) {
    super(null, memory.getNativeBaseOffset(), memory.getRegionOffset(), memory.getCapacity());
    this.memory = memory;
    this.typeId = typeId;
  }

  @Override
  public WritableBuffer writableDuplicate() {
    if (isReadOnly()) {
      throw new ReadOnlyException("Writable duplicate of a read-only Buffer is not allowed.");
    }

    _DrillMemoryBuffer0 buffer = new _DrillMemoryBuffer0(memory, memory.getTypeId() | BUFFER | DUPLICATE);
    buffer.setStartPositionEnd(getStart(), getPosition(), getEnd());
    return buffer;
  }

  @Override
  public WritableBuffer writableDuplicate(ByteOrder byteOrder) {
    if (memory.getDrillBuf().order() == byteOrder) {
      return writableDuplicate();
    }

    if (isReadOnly()) {
      throw new ReadOnlyException("Writable duplicate of a read-only Buffer is not allowed.");
    }

    _DrillMemoryBuffer0 buffer = new _DrillMemoryBuffer0(memory, memory.getTypeId() | BUFFER | DUPLICATE | NONNATIVE);
    buffer.setStartPositionEnd(getStart(), getPosition(), getEnd());
    return buffer;
  }

  @Override
  public WritableBuffer writableRegion() {
    if (isReadOnly()) {
      throw new ReadOnlyException("Writable duplicate of a read-only Buffer is not allowed.");
    }

    _DrillMemory0 memory = this.memory.writableRegion(getPosition(), getEnd() - getPosition());
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public WritableBuffer writableRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    if (isReadOnly()) {
      throw new ReadOnlyException("Writable duplicate of a read-only Buffer is not allowed.");
    }

    _DrillMemory0 memory = this.memory.writableRegion(offsetBytes, capacityBytes, byteOrder);
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public Buffer duplicate() {
    _DrillMemoryBuffer0 buffer = new _DrillMemoryBuffer0(memory, memory.getTypeId() | BUFFER | DUPLICATE | READONLY);
    buffer.setStartPositionEnd(getStart(), getPosition(), getEnd());
    return buffer;
  }

  @Override
  public Buffer duplicate(ByteOrder byteOrder) {
    if (memory.getDrillBuf().order() == byteOrder) {
      return duplicate();
    }

    _DrillMemoryBuffer0 buffer = new _DrillMemoryBuffer0(memory, memory.getTypeId() | BUFFER | DUPLICATE | NONNATIVE | READONLY);
    buffer.setStartPositionEnd(getStart(), getPosition(), getEnd());
    return buffer;
  }

  @Override
  public Buffer region() {
    _DrillMemory0 memory = this.memory.region(getPosition(), getEnd() - getPosition());
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public Buffer region(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    _DrillMemory0 memory = this.memory.region(getPosition(), getEnd() - getPosition());
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public Memory asMemory() {
    return memory;
  }

  @Override
  public WritableMemory asWritableMemory() {
    return memory;
  }

  @Override
  public Object getArray() {
    return null;
  }

  @Override
  public void clear() {
    fill((byte) 0);
  }

  @Override
  public void fill(byte value) {
    long pos = getPosition();
    long len = getEnd() - pos;
    checkInvariants(getStart(), pos + len, getEnd(), getCapacity());
    for (long i = 0; i < len; i++) {
      putByte(pos + i, value);
    }
  }

  @Override
  public int compareTo(long thisOffsetBytes, long thisLengthBytes, Buffer that, long thatOffsetBytes, long thatLengthBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return memory.getMemoryRequestServer();
  }

  @Override
  int getTypeId() {
    return typeId;
  }

  //region Read & write
  @Override
  public boolean getBoolean() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 1);
    return memory.getBoolean((int) pos);
  }

  @Override
  public void putBoolean(boolean value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 1);
    memory.putBoolean(pos, value);
  }

  @Override
  public boolean getBoolean(long offsetBytes) {
    return memory.getBoolean(offsetBytes);
  }

  @Override
  public void putBoolean(long offsetBytes, boolean value) {
    memory.putBoolean(offsetBytes, value);
  }

  @Override
  public void getBooleanArray(boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans) {
    final int size = 1;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthBooleans * size);
    memory.getBooleanArray(pos, dstArray, dstOffsetBooleans, lengthBooleans);
  }

  @Override
  public void putBooleanArray(boolean[] srcArray, int srcOffsetBooleans, int lengthBooleans) {
    final int size = 1;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthBooleans * size);
    memory.putBooleanArray(pos, srcArray, srcOffsetBooleans, lengthBooleans);
  }

  @Override
  public byte getByte() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 1);
    return memory.getByte(pos);
  }

  @Override
  public void putByte(byte value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 1);
    memory.putByte(pos, value);
  }

  @Override
  public byte getByte(long offsetBytes) {
    return memory.getByte(offsetBytes);
  }

  @Override
  public void putByte(long offsetBytes, byte value) {
    memory.putByte(offsetBytes, value);
  }

  @Override
  public void getByteArray(byte[] dstArray, int dstOffsetBytes, int lengthBytes) {
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthBytes);
    memory.getByteArray(pos, dstArray, dstOffsetBytes, lengthBytes);
  }

  @Override
  public void putByteArray(byte[] srcArray, int srcOffsetBytes, int lengthBytes) {
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthBytes);
    memory.putByteArray(pos, srcArray, srcOffsetBytes, lengthBytes);
  }

  @Override
  public char getChar() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 2);
    return memory.getChar(pos);
  }

  @Override
  public void putChar(char value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 2);
    memory.putChar(pos, value);
  }

  @Override
  public char getChar(long offsetBytes) {
    return memory.getChar(offsetBytes);
  }

  @Override
  public void putChar(long offsetBytes, char value) {
    memory.putChar(offsetBytes, value);
  }

  @Override
  public void getCharArray(char[] dstArray, int dstOffsetChars, int lengthChars) {
    final int size = 2;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthChars * size);
    memory.getCharArray(pos, dstArray, dstOffsetChars, lengthChars);
  }

  @Override
  public void putCharArray(char[] srcArray, int srcOffsetChars, int lengthChars) {
    final int size = 2;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthChars * size);
    memory.putCharArray(pos, srcArray, srcOffsetChars, lengthChars);
  }

  @Override
  public double getDouble() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 8);
    return memory.getDouble(pos);
  }

  @Override
  public void putDouble(double value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 8);
    memory.putDouble(pos, value);
  }

  @Override
  public double getDouble(long offsetBytes) {
    return memory.getDouble(offsetBytes);
  }

  @Override
  public void putDouble(long offsetBytes, double value) {
    memory.putDouble(offsetBytes, value);
  }

  @Override
  public void getDoubleArray(double[] dstArray, int dstOffsetDoubles, int lengthDoubles) {
    final int size = 8;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthDoubles * size);
    memory.getDoubleArray(pos, dstArray, dstOffsetDoubles, lengthDoubles);
  }

  @Override
  public void putDoubleArray(double[] srcArray, int srcOffsetDoubles, int lengthDoubles) {
    final int size = 8;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthDoubles * size);
    memory.putDoubleArray(pos, srcArray, srcOffsetDoubles, lengthDoubles);
  }

  @Override
  public float getFloat() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 4);
    return memory.getFloat(pos);
  }

  @Override
  public void putFloat(float value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 4);
    memory.putFloat(pos, value);
  }

  @Override
  public float getFloat(long offsetBytes) {
    return memory.getFloat(offsetBytes);
  }

  @Override
  public void putFloat(long offsetBytes, float value) {
    memory.putFloat(offsetBytes, value);
  }

  @Override
  public void getFloatArray(float[] dstArray, int dstOffsetFloats, int lengthFloats) {
    final int size = 4;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthFloats * size);
    memory.getFloatArray(pos, dstArray, dstOffsetFloats, lengthFloats);
  }

  @Override
  public void putFloatArray(float[] srcArray, int srcOffsetFloats, int lengthFloats) {
    final int size = 4;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthFloats * size);
    memory.putFloatArray(pos, srcArray, srcOffsetFloats, lengthFloats);
  }

  @Override
  public int getInt() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 4);
    return memory.getInt(pos);
  }

  @Override
  public void putInt(int value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 4);
    memory.putInt(pos, value);
  }

  @Override
  public int getInt(long offsetBytes) {
    return memory.getInt(offsetBytes);
  }

  @Override
  public void putInt(long offsetBytes, int value) {
    memory.putInt(offsetBytes, value);
  }

  @Override
  public void getIntArray(int[] dstArray, int dstOffsetInts, int lengthInts) {
    final int size = 4;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthInts * size);
    memory.getIntArray(pos, dstArray, dstOffsetInts, lengthInts);
  }

  @Override
  public void putIntArray(int[] srcArray, int srcOffsetInts, int lengthInts) {
    final int size = 4;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthInts * size);
    memory.putIntArray(pos, srcArray, srcOffsetInts, lengthInts);
  }

  @Override
  public long getLong() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 8);
    return memory.getLong(pos);
  }

  @Override
  public void putLong(long value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 8);
    memory.putLong(pos, value);
  }

  @Override
  public long getLong(long offsetBytes) {
    return memory.getLong(offsetBytes);
  }

  @Override
  public void putLong(long offsetBytes, long value) {
    memory.putLong(offsetBytes, value);
  }

  @Override
  public void getLongArray(long[] dstArray, int dstOffsetLongs, int lengthLongs) {
    final int size = 8;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthLongs * size);
    memory.getLongArray(pos, dstArray, dstOffsetLongs, lengthLongs);
  }

  @Override
  public void putLongArray(long[] srcArray, int srcOffsetLongs, int lengthLongs) {
    final int size = 8;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthLongs * size);
    memory.putLongArray(pos, srcArray, srcOffsetLongs, lengthLongs);
  }

  @Override
  public short getShort() {
    long pos = getPosition();
    incrementAndAssertPositionForRead(pos, 2);
    return memory.getShort(pos);
  }

  @Override
  public void putShort(short value) {
    long pos = getPosition();
    incrementAndAssertPositionForWrite(pos, 2);
    memory.putShort(pos, value);
  }

  @Override
  public short getShort(long offsetBytes) {
    return memory.getShort(offsetBytes);
  }

  @Override
  public void putShort(long offsetBytes, short value) {
    memory.putShort(offsetBytes, value);
  }

  @Override
  public void getShortArray(short[] dstArray, int dstOffsetShorts, int lengthShorts) {
    final int size = 2;
    long pos = getPosition();
    incrementAndCheckPositionForRead(pos, lengthShorts * size);
    memory.getShortArray(pos, dstArray, dstOffsetShorts, lengthShorts);
  }

  @Override
  public void putShortArray(short[] srcArray, int srcOffsetShorts, int lengthShorts) {
    final int size = 2;
    long pos = getPosition();
    incrementAndCheckPositionForWrite(pos, lengthShorts * size);
    memory.putShortArray(pos, srcArray, srcOffsetShorts, lengthShorts);
  }
  //endregion
}
