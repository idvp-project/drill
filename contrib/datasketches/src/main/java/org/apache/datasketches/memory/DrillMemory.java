package org.apache.datasketches.memory;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 *
 */
abstract class DrillMemory extends WritableMemory {

  private final DrillBuf drillBuf;
  private final ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;

  DrillMemory(final DrillBuf drillBuf) {
    super(null, drillBuf.memoryAddress(), 0, drillBuf.capacity());
    this.drillBuf = drillBuf;
  }

  @Override
  public Memory region(long offsetBytes, long capacityBytes) {
    DrillBuf slice = drillBuf.slice((int) offsetBytes, (int) capacityBytes);
    return regionImpl(slice, null);
  }

  @Override
  public Memory region(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    DrillBuf slice = drillBuf.slice((int) offsetBytes, (int) capacityBytes);
    return regionImpl(slice, byteOrder);
  }

  @Override
  public Buffer asBuffer() {
    return null;
  }

  @Override
  public Buffer asBuffer(ByteOrder byteOrder) {
    return null;
  }

  @Override
  public ByteBuffer unsafeByteBufferView(long offsetBytes, int capacityBytes) {
    return null;
  }

  @Override
  public int compareTo(long thisOffsetBytes, long thisLengthBytes, Memory that, long thatOffsetBytes, long thatLengthBytes) {
    return 0;
  }

  @Override
  public void copyTo(long srcOffsetBytes, WritableMemory destination, long dstOffsetBytes, long lengthBytes) {

  }

  @Override
  public void writeTo(long offsetBytes, long lengthBytes, WritableByteChannel out) throws IOException {

  }

  @Override
  public WritableMemory writableRegion(long offsetBytes, long capacityBytes) {
    return null;
  }

  @Override
  public WritableMemory writableRegion(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    return null;
  }

  @Override
  public WritableBuffer asWritableBuffer() {
    return null;
  }

  @Override
  public WritableBuffer asWritableBuffer(ByteOrder byteOrder) {
    return null;
  }

  @Override
  public boolean getBoolean(long offsetBytes) {
    return drillBuf.getBoolean((int) offsetBytes);
  }

  @Override
  public void putBoolean(long offsetBytes, boolean value) {
    drillBuf.setBoolean((int) offsetBytes, value);
  }

  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans) {
    checkBounds(dstOffsetBooleans, lengthBooleans, dstArray.length);
    for (int i = 0; i < lengthBooleans; i++) {
      dstArray[dstOffsetBooleans + i] = drillBuf.getBoolean((int) offsetBytes + i);
    }
  }

  @Override
  public void putBooleanArray(long offsetBytes, boolean[] srcArray, int srcOffsetBooleans, int lengthBooleans) {
    checkBounds(srcOffsetBooleans, lengthBooleans, srcArray.length);
    checkForWrite(offsetBytes, lengthBooleans);
    for (int i = 0; i < lengthBooleans; i++) {
      drillBuf.setBoolean((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public byte getByte(long offsetBytes) {
    return drillBuf.getByte((int) offsetBytes);
  }

  @Override
  public void putByte(long offsetBytes, byte value) {
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
    checkForWrite(offsetBytes, lengthBytes);
    drillBuf.setBytes((int) offsetBytes, srcArray, srcOffsetBytes, lengthBytes);
  }


  @Override
  public char getChar(long offsetBytes) {
    return drillBuf.getChar((int) offsetBytes);
  }

  @Override
  public void putChar(long offsetBytes, char value) {
    drillBuf.setChar((int) offsetBytes, value);
  }

  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffsetChars, int lengthChars) {
    checkBounds(dstOffsetChars, lengthChars, dstArray.length);
    for (int i = 0; i < lengthChars; i++) {
      dstArray[dstOffsetChars + i] = drillBuf.getChar((int) offsetBytes + i);
    }
  }

  @Override
  public void putCharArray(long offsetBytes, char[] srcArray, int srcOffsetChars, int lengthChars) {
    checkBounds(srcOffsetChars, lengthChars, srcArray.length);
    checkForWrite(offsetBytes, lengthChars * 2);
    for (int i = 0; i < lengthChars; i++) {
      drillBuf.setChar((int) offsetBytes + i, srcArray[i]);
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
    checkForWrite(offsetBytes, bytes.length);
    drillBuf.setBytes((int) offsetBytes, bytes);
    return bytes.length;
  }

  @Override
  public double getDouble(long offsetBytes) {
    return drillBuf.getDouble((int) offsetBytes);
  }

  @Override
  public void putDouble(long offsetBytes, double value) {
    drillBuf.setDouble((int) offsetBytes, value);
  }

  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffsetDoubles, int lengthDoubles) {
    checkBounds(dstOffsetDoubles, lengthDoubles, dstArray.length);
    for (int i = 0; i < lengthDoubles; i++) {
      dstArray[dstOffsetDoubles + i] = drillBuf.getDouble((int) offsetBytes + i);
    }
  }

  @Override
  public void putDoubleArray(long offsetBytes, double[] srcArray, int srcOffsetDoubles, int lengthDoubles) {
    checkBounds(srcOffsetDoubles, lengthDoubles, srcArray.length);
    checkForWrite(offsetBytes, lengthDoubles * 8);
    for (int i = 0; i < lengthDoubles; i++) {
      drillBuf.setDouble((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public float getFloat(long offsetBytes) {
    return drillBuf.getFloat((int) offsetBytes);
  }

  @Override
  public void putFloat(long offsetBytes, float value) {
    drillBuf.setFloat((int) offsetBytes, value);
  }

  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffsetFloats, int lengthFloats) {
    checkBounds(dstOffsetFloats, lengthFloats, dstArray.length);
    for (int i = 0; i < lengthFloats; i++) {
      dstArray[dstOffsetFloats + i] = drillBuf.getFloat((int) offsetBytes + i);
    }
  }

  @Override
  public void putFloatArray(long offsetBytes, float[] srcArray, int srcOffsetFloats, int lengthFloats) {
    checkBounds(srcOffsetFloats, lengthFloats, srcArray.length);
    checkForWrite(offsetBytes, lengthFloats * 4);
    for (int i = 0; i < lengthFloats; i++) {
      drillBuf.setFloat((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public int getInt(long offsetBytes) {
    return drillBuf.getInt((int) offsetBytes);
  }

  @Override
  public void putInt(long offsetBytes, int value) {
    drillBuf.setInt((int) offsetBytes, value);
  }

  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffsetInts, int lengthInts) {
    checkBounds(dstOffsetInts, lengthInts, dstArray.length);
    for (int i = 0; i < lengthInts; i++) {
      dstArray[dstOffsetInts + i] = drillBuf.getInt((int) offsetBytes + i);
    }
  }

  @Override
  public void putIntArray(long offsetBytes, int[] srcArray, int srcOffsetInts, int lengthInts) {
    checkBounds(srcOffsetInts, lengthInts, srcArray.length);
    checkForWrite(offsetBytes, lengthInts * 4);
    for (int i = 0; i < lengthInts; i++) {
      drillBuf.setFloat((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public long getLong(long offsetBytes) {
    return drillBuf.getLong((int) offsetBytes);
  }

  @Override
  public void putLong(long offsetBytes, long value) {
    drillBuf.setLong((int) offsetBytes, value);
  }

  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffsetLongs, int lengthLongs) {
    checkBounds(dstOffsetLongs, lengthLongs, dstArray.length);
    for (int i = 0; i < lengthLongs; i++) {
      dstArray[dstOffsetLongs + i] = drillBuf.getLong((int) offsetBytes + i);
    }
  }

  @Override
  public void putLongArray(long offsetBytes, long[] srcArray, int srcOffsetLongs, int lengthLongs) {
    checkBounds(srcOffsetLongs, lengthLongs, srcArray.length);
    checkForWrite(offsetBytes, lengthLongs * 8);
    for (int i = 0; i < lengthLongs; i++) {
      drillBuf.setFloat((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public short getShort(long offsetBytes) {
    return drillBuf.getShort((int) offsetBytes);
  }

  @Override
  public void putShort(long offsetBytes, short value) {
    drillBuf.setShort((int) offsetBytes, value);
  }

  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffsetShorts, int lengthShorts) {
    checkBounds(dstOffsetShorts, lengthShorts, dstArray.length);
    for (int i = 0; i < lengthShorts; i++) {
      dstArray[dstOffsetShorts + i] = drillBuf.getShort((int) offsetBytes + i);
    }
  }

  @Override
  public void putShortArray(long offsetBytes, short[] srcArray, int srcOffsetShorts, int lengthShorts) {
    checkBounds(srcOffsetShorts, lengthShorts, srcArray.length);
    checkForWrite(offsetBytes, lengthShorts * 2);
    for (int i = 0; i < lengthShorts; i++) {
      drillBuf.setFloat((int) offsetBytes + i, srcArray[i]);
    }
  }

  @Override
  public long getAndAddLong(long offsetBytes, long delta) {
    return 0;
  }

  @Override
  public boolean compareAndSwapLong(long offsetBytes, long expect, long update) {
    return false;
  }

  @Override
  public long getAndSetLong(long offsetBytes, long newValue) {
    return 0;
  }

  @Override
  public Object getArray() {
    return null;
  }

  @Override
  public void clear() {

  }

  @Override
  public void clear(long offsetBytes, long lengthBytes) {

  }

  @Override
  public void clearBits(long offsetBytes, byte bitMask) {

  }

  @Override
  public void fill(byte value) {

  }

  @Override
  public void fill(long offsetBytes, long lengthBytes, byte value) {

  }

  @Override
  public void setBits(long offsetBytes, byte bitMask) {

  }


  @Override
  int getTypeId() {
    return 0;
  }

  /**
   * Check the requested offset and length against the allocated size.
   * The invariants equation is: {@code 0 <= reqOff <= reqLen <= reqOff + reqLen <= allocSize}.
   * If this equation is violated an {@link IllegalArgumentException} will be thrown.
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size.
   */
  private void checkBounds(final long reqOff, final long reqLen, final long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new IllegalArgumentException(
              "reqOffset: " + reqOff + ", reqLength: " + reqLen
                      + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  private void checkForWrite(final long offsetBytes, final long lengthBytes) {
    checkValid();
    checkBounds(offsetBytes, lengthBytes, getCapacity());
    if (isReadOnly()) {
      throw new ReadOnlyException("Memory is read-only.");
    }
  }

}
