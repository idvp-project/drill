package org.apache.datasketches.memory;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * Alternative implementation of {@link DrillWritableMemory}.
 * Unlike {@link DrillWritableMemory}, {@link _DrillMemory0} is not based on the standard implementation
 * of datasketckes {@link WritableMemoryImpl}. Instead it delegates memory access method call to appropriate {@link DrillBuf} method.
 */
public final class _DrillMemory0 extends WritableMemory {

  private final DrillBuf drillBuf;
  private final _DrillMemoryRequestServer0 requestServer;
  private final int typeId;

  public static _DrillMemory0 wrap(DrillBuf buf) {
    return new _DrillMemory0(buf, false);
  }

  public static _DrillMemory0 wrapReadonly(DrillBuf buf) {
    return new _DrillMemory0(buf, DIRECT | MEMORY | NATIVE | READONLY, false);
  }

  /**
   * Сan an DrillWritableMemory be closed by DrillMemoryRequestServer.
   * DrillMemoryRequestServer cannot release DrillBuf if it was created externally (e.g. from OperationContext::getManagedBuffer).
   */
  final boolean closeable;

  _DrillMemory0(DrillBuf drillBuf, boolean closeable) {
    this(drillBuf, DIRECT | MEMORY | NATIVE, closeable);
  }

  private _DrillMemory0(DrillBuf drillBuf, int typeId, boolean closeable) {
    super(null, drillBuf.memoryAddress(), 0, drillBuf.capacity());
    this.drillBuf = drillBuf;
    this.requestServer = new _DrillMemoryRequestServer0(drillBuf.alloc());
    this.typeId = typeId;
    this.closeable = closeable;
  }

  @Override
  public _DrillMemory0 region(long offsetBytes, long capacityBytes) {
    return createRegion(offsetBytes, capacityBytes, drillBuf.order(), true);
  }

  @Override
  public _DrillMemory0 region(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    return createRegion(offsetBytes, capacityBytes, byteOrder, true);
  }

  @Override
  public _DrillMemory0 writableRegion(long offsetBytes, long capacityBytes) {
    return createRegion(offsetBytes, capacityBytes, drillBuf.order(), false);
  }

  @Override
  public _DrillMemory0 writableRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    return createRegion(offsetBytes, capacityBytes, byteOrder, false);
  }

  private _DrillMemory0 createRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder, boolean readOnly) {
    if (isReadOnly() && !readOnly) {
      throw new ReadOnlyException("Writable region of a read-only Memory is not allowed.");
    }

    checkValid();
    checkBounds(offsetBytes, capacityBytes, getCapacity());

    int type = DIRECT | MEMORY | REGION;
    if (readOnly) {
      type |= READONLY;
    }
    if (byteOrder != drillBuf.order()) {
      type |= NONNATIVE;
    }

    DrillBuf slice = drillBuf.slice((int) offsetBytes, (int) capacityBytes);
    return new _DrillMemory0(slice, type, closeable);
  }

  @Override
  public Buffer asBuffer() {
    return new _DrillMemoryBuffer0(this);
  }

  @Override
  public Buffer asBuffer(ByteOrder byteOrder) {
    if (drillBuf.order() == byteOrder) {
      return asBuffer();
    }

    _DrillMemory0 memory = new _DrillMemory0(drillBuf, getTypeId() | NONNATIVE, closeable);
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public ByteBuffer unsafeByteBufferView(long offsetBytes, int capacityBytes) {
    return drillBuf.nioBuffer((int) offsetBytes, capacityBytes);
  }

  @Override
  public WritableBuffer asWritableBuffer() {
    if (isReadOnly()) {
      throw new ReadOnlyException("Writable buffer of a read-only Memory is not allowed.");
    }

    return new _DrillMemoryBuffer0(this);
  }

  @Override
  public WritableBuffer asWritableBuffer(ByteOrder byteOrder) {
    if (drillBuf.order() == byteOrder) {
      return asWritableBuffer();
    }

    if (isReadOnly()) {
      throw new ReadOnlyException("Writable buffer of a read-only Memory is not allowed.");
    }

    _DrillMemory0 memory = new _DrillMemory0(drillBuf, getTypeId() | NONNATIVE, closeable);
    return new _DrillMemoryBuffer0(memory);
  }

  @Override
  public void copyTo(long srcOffsetBytes, WritableMemory destination, long dstOffsetBytes, long lengthBytes) {
    if (destination instanceof _DrillMemory0) {
      ((_DrillMemory0) destination).getDrillBuf().setBytes((int) srcOffsetBytes, getDrillBuf(), (int) dstOffsetBytes, (int) lengthBytes);
    }

    final int chunkLength = (int) Math.min(lengthBytes, 4096);
    byte[] chunk = new byte[chunkLength];
    while (lengthBytes > 0) {
      int currentLength = (int) Math.min(lengthBytes, chunkLength);
      drillBuf.getBytes((int) srcOffsetBytes, chunk, 0, chunkLength);
      destination.putByteArray(dstOffsetBytes, chunk, 0, chunkLength);
      lengthBytes -= currentLength;
      srcOffsetBytes += currentLength;
      dstOffsetBytes += currentLength;
    }
  }

  @Override
  public int compareTo(long thisOffsetBytes, long thisLengthBytes, Memory that, long thatOffsetBytes, long thatLengthBytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeTo(long offsetBytes, long lengthBytes, WritableByteChannel out) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return requestServer;
  }

  @Override
  long getNativeBaseOffset() {
    return drillBuf.memoryAddress();
  }

  @Override
  public long getAndAddLong(long offsetBytes, long delta) {
    //todo: Atomic?
    long current;
    do {
      current = getLong(offsetBytes);
    } while (!compareAndSwapLong(offsetBytes, current, current + delta));
    return current;
  }

  @Override
  public boolean compareAndSwapLong(long offsetBytes, long expect, long update) {
    //todo: Atomic?
    long current = getLong(offsetBytes);
    if (expect == current) {
      putLong(offsetBytes, update);
      return true;
    }
    return false;
  }

  @Override
  public long getAndSetLong(long offsetBytes, long newValue) {
    long current = getLong(offsetBytes);
    putLong(offsetBytes, newValue);
    return current;
  }

  @Override
  public Object getArray() {
    return null;
  }

  @Override
  public void clear() {
    fill(0, getCapacity(), (byte) 0);
  }

  @Override
  public void clear(long offsetBytes, long lengthBytes) {
    fill(offsetBytes, lengthBytes, (byte) 0);
  }

  @Override
  public void clearBits(long offsetBytes, byte bitMask) {
    byte b = getByte(offsetBytes);
    b &= ~bitMask;
    putByte(offsetBytes, b);
  }

  @Override
  public void fill(byte value) {
    fill(0, drillBuf.capacity(), value);
  }

  @Override
  public void fill(long offsetBytes, long lengthBytes, byte value) {
    for (long i = offsetBytes; i < offsetBytes + lengthBytes; i++) {
      putByte(i, value);
    }
  }

  @Override
  public void setBits(long offsetBytes, byte bitMask) {
    byte b = getByte(offsetBytes);
    b |= bitMask;
    putByte(offsetBytes, b);
  }


  @Override
  int getTypeId() {
    return typeId;
  }

  DrillBuf getDrillBuf() {
    return drillBuf;
  }

  //region Read & Write values
  @Override
  public boolean getBoolean(long offsetBytes) {
    return drillBuf.getBoolean((int) offsetBytes);
  }

  @Override
  public void putBoolean(long offsetBytes, boolean value) {
    checkBoundsForWrite(offsetBytes, 1);
    drillBuf.setBoolean((int) offsetBytes, value);
  }

  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans) {
    final int size = 1;
    checkBounds(dstOffsetBooleans, lengthBooleans, dstArray.length);
    for (int i = 0; i < lengthBooleans; i++) {
      dstArray[dstOffsetBooleans + i] = drillBuf.getBoolean((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffsetBooleans, int lengthBooleans) {
    final int size = 1;
    checkBounds(srcOffsetBooleans, lengthBooleans, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthBooleans);
    for (int i = 0; i < lengthBooleans; i++) {
      drillBuf.setBoolean((int) offsetBytes + i * size, srcArray[srcOffsetBooleans + i]);
    }
  }

  @Override
  public byte getByte(long offsetBytes) {
    return drillBuf.getByte((int) offsetBytes);
  }

  @Override
  public void putByte(long offsetBytes, byte value) {
    checkBoundsForWrite(offsetBytes, 1);
    drillBuf.setByte((int) offsetBytes, value);
  }

  @Override
  public void getByteArray(long offsetBytes, byte[] dstArray, int dstOffsetBytes, int lengthBytes) {
    checkBounds(dstOffsetBytes, lengthBytes, dstArray.length);
    drillBuf.getBytes((int) offsetBytes, dstArray, dstOffsetBytes, lengthBytes);
  }

  @Override
  public void putByteArray(long offsetBytes, byte[] srcArray, int srcOffsetBytes, int lengthBytes) {
    checkBounds(srcOffsetBytes, lengthBytes, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthBytes);
    drillBuf.setBytes((int) offsetBytes, srcArray, srcOffsetBytes, lengthBytes);
  }


  @Override
  public char getChar(long offsetBytes) {
    return drillBuf.getChar((int) offsetBytes);
  }

  @Override
  public void putChar(long offsetBytes, char value) {
    checkBoundsForWrite(offsetBytes, 2);
    drillBuf.setChar((int) offsetBytes, value);
  }

  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffsetChars, int lengthChars) {
    final int size = 2;
    checkBounds(dstOffsetChars, lengthChars, dstArray.length);
    for (int i = 0; i < lengthChars; i++) {
      dstArray[dstOffsetChars + i] = drillBuf.getChar((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putCharArray(long offsetBytes, char[] srcArray, int srcOffsetChars, int lengthChars) {
    final int size = 2;
    checkBounds(srcOffsetChars, lengthChars, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthChars * size);
    for (int i = 0; i < lengthChars; i++) {
      drillBuf.setChar((int) offsetBytes + i * size, srcArray[srcOffsetChars + i]);
    }
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, Appendable dst) throws IOException, Utf8CodingException {
    byte[] bytes = new byte[utf8LengthBytes];
    drillBuf.getBytes((int) offsetBytes, bytes);
    String str = new String(bytes, StandardCharsets.UTF_8);
    dst.append(str);
    return str.length();
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, StringBuilder dst) throws Utf8CodingException {
    byte[] bytes = new byte[utf8LengthBytes];
    drillBuf.getBytes((int) offsetBytes, bytes);
    String str = new String(bytes, StandardCharsets.UTF_8);
    dst.append(str);
    return str.length();
  }

  @Override
  public long putCharsToUtf8(long offsetBytes, CharSequence src) {
    byte[] bytes = src.toString().getBytes(StandardCharsets.UTF_8);
    checkBoundsForWrite(offsetBytes, bytes.length);
    drillBuf.setBytes((int) offsetBytes, bytes);
    return bytes.length;
  }

  @Override
  public double getDouble(long offsetBytes) {
    return drillBuf.getDouble((int) offsetBytes);
  }

  @Override
  public void putDouble(long offsetBytes, double value) {
    checkBoundsForWrite(offsetBytes, 8);
    drillBuf.setDouble((int) offsetBytes, value);
  }

  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffsetDoubles, int lengthDoubles) {
    final int size = 8;
    checkBounds(dstOffsetDoubles, lengthDoubles, dstArray.length);
    for (int i = 0; i < lengthDoubles; i++) {
      dstArray[dstOffsetDoubles + i] = drillBuf.getDouble((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffsetDoubles, int lengthDoubles) {
    final int size = 8;
    checkBounds(srcOffsetDoubles, lengthDoubles, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthDoubles * size);
    for (int i = 0; i < lengthDoubles; i++) {
      drillBuf.setDouble((int) offsetBytes + i * size, srcArray[srcOffsetDoubles + i]);
    }
  }

  @Override
  public float getFloat(long offsetBytes) {
    return drillBuf.getFloat((int) offsetBytes);
  }

  @Override
  public void putFloat(long offsetBytes, float value) {
    checkBoundsForWrite(offsetBytes, 4);
    drillBuf.setFloat((int) offsetBytes, value);
  }

  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffsetFloats, int lengthFloats) {
    final int size = 4;
    checkBounds(dstOffsetFloats, lengthFloats, dstArray.length);
    for (int i = 0; i < lengthFloats; i++) {
      dstArray[dstOffsetFloats + i] = drillBuf.getFloat((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffsetFloats, int lengthFloats) {
    final int size = 4;
    checkBounds(srcOffsetFloats, lengthFloats, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthFloats * size);
    for (int i = 0; i < lengthFloats; i++) {
      drillBuf.setFloat((int) offsetBytes + i * size, srcArray[srcOffsetFloats + i]);
    }
  }

  @Override
  public int getInt(long offsetBytes) {
    return drillBuf.getInt((int) offsetBytes);
  }

  @Override
  public void putInt(long offsetBytes, int value) {
    checkBoundsForWrite(offsetBytes, 4);
    drillBuf.setInt((int) offsetBytes, value);
  }

  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffsetInts, int lengthInts) {
    final int size = 4;
    checkBounds(dstOffsetInts, lengthInts, dstArray.length);
    for (int i = 0; i < lengthInts; i++) {
      dstArray[dstOffsetInts + i] = drillBuf.getInt((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putIntArray(long offsetBytes, int[] srcArray, int srcOffsetInts, int lengthInts) {
    final int size = 4;
    checkBounds(srcOffsetInts, lengthInts, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthInts * size);
    for (int i = 0; i < lengthInts; i++) {
      drillBuf.setInt((int) offsetBytes + i * size, srcArray[srcOffsetInts + i]);
    }
  }

  @Override
  public long getLong(long offsetBytes) {
    return drillBuf.getLong((int) offsetBytes);
  }

  @Override
  public void putLong(long offsetBytes, long value) {
    checkBoundsForWrite(offsetBytes, 8);
    drillBuf.setLong((int) offsetBytes, value);
  }

  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffsetLongs, int lengthLongs) {
    final int size = 8;
    checkBounds(dstOffsetLongs, lengthLongs, dstArray.length);
    for (int i = 0; i < lengthLongs; i++) {
      dstArray[dstOffsetLongs + i] = drillBuf.getLong((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putLongArray(long offsetBytes, long[] srcArray, int srcOffsetLongs, int lengthLongs) {
    final int size = 8;
    checkBounds(srcOffsetLongs, lengthLongs, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthLongs * size);
    for (int i = 0; i < lengthLongs; i++) {
      drillBuf.setLong((int) offsetBytes + i * size, srcArray[srcOffsetLongs + i]);
    }
  }

  @Override
  public short getShort(long offsetBytes) {
    return drillBuf.getShort((int) offsetBytes);
  }

  @Override
  public void putShort(long offsetBytes, short value) {
    checkBoundsForWrite(offsetBytes, 2);
    drillBuf.setShort((int) offsetBytes, value);
  }

  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffsetShorts, int lengthShorts) {
    final int size = 2;
    checkBounds(dstOffsetShorts, lengthShorts, dstArray.length);
    for (int i = 0; i < lengthShorts; i++) {
      dstArray[dstOffsetShorts + i] = drillBuf.getShort((int) offsetBytes + i * size);
    }
  }

  @Override
  public void putShortArray(long offsetBytes, short[] srcArray, int srcOffsetShorts, int lengthShorts) {
    final int size = 2;
    checkBounds(srcOffsetShorts, lengthShorts, srcArray.length);
    checkBoundsForWrite(offsetBytes, lengthShorts * size);
    for (int i = 0; i < lengthShorts; i++) {
      drillBuf.setShort((int) offsetBytes + i * size, srcArray[srcOffsetShorts + i]);
    }
  }
  //endregion

  private void checkBounds(long reqOff, long reqLen, long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new IllegalArgumentException("reqOffset: " + reqOff + ", reqLength: " + reqLen + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  private void checkBoundsForWrite(long offsetBytes, long lengthBytes) {
    checkValid();
    checkBounds(offsetBytes, lengthBytes, getCapacity());
    if (isReadOnly()) {
      throw new ReadOnlyException("Memory is read-only.");
    }
  }


}
