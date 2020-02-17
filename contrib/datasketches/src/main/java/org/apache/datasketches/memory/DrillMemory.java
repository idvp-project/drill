package org.apache.datasketches.memory;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

import static org.apache.datasketches.memory.UnsafeUtil.checkBounds;

/**
 *
 */
abstract class DrillMemory extends Memory {

  private final DrillBuf drillBuf;

  DrillMemory(final DrillBuf drillBuf) {
    super(null, drillBuf.memoryAddress(), 0, drillBuf.capacity());
    this.drillBuf = drillBuf;
  }

  private DrillMemory(DrillBuf drillBuf, long offset, long capacityBytes) {
    super(null, drillBuf.memoryAddress(), offset, capacityBytes);
    this.drillBuf = drillBuf;
  }

  @Override
  public Memory region(long offsetBytes, long capacityBytes) {
    return null;
  }

  @Override
  public Memory region(long offsetBytes, long capacityBytes, ByteOrder byteOrder) {
    return null;
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
  public boolean getBoolean(long offsetBytes) {
    return drillBuf.getBoolean((int) offsetBytes);

  }

  @Override
  public void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffsetBooleans, int lengthBooleans) {
    checkBounds(dstOffsetBooleans, lengthBooleans, dstArray.length);
    for (int i = 0; i < lengthBooleans; i++) {
      dstArray[dstOffsetBooleans + i] = drillBuf.getBoolean((int) offsetBytes + i);
    }
  }

  @Override
  public byte getByte(long offsetBytes) {
    return drillBuf.getByte((int) offsetBytes);
  }

  @Override
  public void getByteArray(long offsetBytes, byte[] dstArray, int dstOffsetBytes, int lengthBytes) {
    checkBounds(dstOffsetBytes, lengthBytes, dstArray.length);
    drillBuf.getBytes((int) offsetBytes, dstArray, dstOffsetBytes, lengthBytes);
  }

  @Override
  public char getChar(long offsetBytes) {
    return drillBuf.getChar((int) offsetBytes);
  }

  @Override
  public void getCharArray(long offsetBytes, char[] dstArray, int dstOffsetChars, int lengthChars) {
    checkBounds(dstOffsetChars, lengthChars, dstArray.length);
    for (int i = 0; i < lengthChars; i++) {
      dstArray[dstOffsetChars + i] = drillBuf.getChar((int) offsetBytes + i);
    }
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, Appendable dst) throws IOException, Utf8CodingException {
    return 0;
  }

  @Override
  public int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, StringBuilder dst) throws Utf8CodingException {
    return 0;
  }

  @Override
  public double getDouble(long offsetBytes) {
    return drillBuf.getDouble((int) offsetBytes);
  }

  @Override
  public void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffsetDoubles, int lengthDoubles) {
    checkBounds(dstOffsetDoubles, lengthDoubles, dstArray.length);
    for (int i = 0; i < lengthDoubles; i++) {
      dstArray[dstOffsetDoubles + i] = drillBuf.getDouble((int) offsetBytes + i);
    }
  }

  @Override
  public float getFloat(long offsetBytes) {
    return drillBuf.getFloat((int) offsetBytes);
  }

  @Override
  public void getFloatArray(long offsetBytes, float[] dstArray, int dstOffsetFloats, int lengthFloats) {
    checkBounds(dstOffsetFloats, lengthFloats, dstArray.length);
    for (int i = 0; i < lengthFloats; i++) {
      dstArray[dstOffsetFloats + i] = drillBuf.getFloat((int) offsetBytes + i);
    }
  }

  @Override
  public int getInt(long offsetBytes) {
    return drillBuf.getInt((int) offsetBytes);
  }

  @Override
  public void getIntArray(long offsetBytes, int[] dstArray, int dstOffsetInts, int lengthInts) {
    checkBounds(dstOffsetInts, lengthInts, dstArray.length);
    for (int i = 0; i < lengthInts; i++) {
      dstArray[dstOffsetInts + i] = drillBuf.getInt((int) offsetBytes + i);
    }
  }

  @Override
  public long getLong(long offsetBytes) {
    return drillBuf.getLong((int) offsetBytes);
  }

  @Override
  public void getLongArray(long offsetBytes, long[] dstArray, int dstOffsetLongs, int lengthLongs) {
    checkBounds(dstOffsetLongs, lengthLongs, dstArray.length);
    for (int i = 0; i < lengthLongs; i++) {
      dstArray[dstOffsetLongs + i] = drillBuf.getLong((int) offsetBytes + i);
    }
  }

  @Override
  public short getShort(long offsetBytes) {
    return drillBuf.getShort((int) offsetBytes);
  }

  @Override
  public void getShortArray(long offsetBytes, short[] dstArray, int dstOffsetShorts, int lengthShorts) {
    checkBounds(dstOffsetShorts, lengthShorts, dstArray.length);
    for (int i = 0; i < lengthShorts; i++) {
      dstArray[dstOffsetShorts + i] = drillBuf.getShort((int) offsetBytes + i);
    }
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
  MemoryRequestServer getMemoryRequestServer() {
    return null;
  }

  @Override
  int getTypeId() {
    return 0;
  }
}
