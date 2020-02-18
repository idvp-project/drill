package org.apache.datasketches.memory;

import io.netty.buffer.ByteBufAllocator;
import org.apache.drill.categories.MemoryTest;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.BaseTest;
import org.apache.drill.test.OperatorFixture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Copy of datasketches CommonBufferTest for {@link _DrillMemoryBuffer0}.
 */
@Category(MemoryTest.class)
public class CommonBufferTest extends BaseTest {

  @Rule
  public final BaseDirTestWatcher baseDirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void _checkSetGet0() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      _DrillMemoryRequestServer0 requestServer = new _DrillMemoryRequestServer0(byteBufAllocator);

      WritableMemory mem = requestServer.request(64);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 64);
        setGetTests(buf);
        setGetTests2(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void _checkSetGetArrays0() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      _DrillMemoryRequestServer0 requestServer = new _DrillMemoryRequestServer0(byteBufAllocator);

      WritableMemory mem = requestServer.request(32);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 32);
        setGetArraysTests(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void _checkSetGetPartialArraysWithOffset0() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      _DrillMemoryRequestServer0 requestServer = new _DrillMemoryRequestServer0(byteBufAllocator);

      WritableMemory mem = requestServer.request(32);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 32);
        setGetPartialArraysWithOffsetTests(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void _checkSetClearMemoryRegions0() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      _DrillMemoryRequestServer0 requestServer = new _DrillMemoryRequestServer0(byteBufAllocator);

      WritableMemory mem = requestServer.request(64);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 64);
        setClearMemoryRegionsTests(buf);
        buf.resetPosition();
        for (int i = 0; i < 64; i++) {
          assertEquals(mem.getByte(i), 0);
        }
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void checkSetGet() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      DrillMemoryRequestServer requestServer = new DrillMemoryRequestServer(byteBufAllocator);

      WritableMemory mem = requestServer.request(64);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 64);
        setGetTests(buf);
        setGetTests2(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void checkSetGetArrays() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      DrillMemoryRequestServer requestServer = new DrillMemoryRequestServer(byteBufAllocator);

      WritableMemory mem = requestServer.request(32);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 32);
        setGetArraysTests(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void checkSetGetPartialArraysWithOffset() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      DrillMemoryRequestServer requestServer = new DrillMemoryRequestServer(byteBufAllocator);

      WritableMemory mem = requestServer.request(32);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 32);
        setGetPartialArraysWithOffsetTests(buf);
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  @Test
  public void checkSetClearMemoryRegions() throws Exception {
    try (OperatorFixture operatorFixture = new OperatorFixture.Builder(baseDirTestWatcher).build()) {
      ByteBufAllocator byteBufAllocator = operatorFixture.allocator().getAsByteBufAllocator();

      DrillMemoryRequestServer requestServer = new DrillMemoryRequestServer(byteBufAllocator);

      WritableMemory mem = requestServer.request(64);
      try {
        WritableBuffer buf = mem.asWritableBuffer();
        assertEquals(buf.getCapacity(), 64);
        setClearMemoryRegionsTests(buf);
        buf.resetPosition();
        for (int i = 0; i < 64; i++) {
          assertEquals(mem.getByte(i), 0);
        }
      } finally {
        requestServer.requestClose(mem, null);
      }
    }
  }

  public static void setGetTests(WritableBuffer buf) {
    buf.putBoolean(true);
    buf.putBoolean(false);
    buf.putByte((byte) -1);
    buf.putByte((byte) 0);
    buf.putChar('A');
    buf.putChar('Z');
    buf.putShort(Short.MAX_VALUE);
    buf.putShort(Short.MIN_VALUE);
    buf.putInt(Integer.MAX_VALUE);
    buf.putInt(Integer.MIN_VALUE);
    buf.putFloat(Float.MAX_VALUE);
    buf.putFloat(Float.MIN_VALUE);
    buf.putLong(Long.MAX_VALUE);
    buf.putLong(Long.MIN_VALUE);
    buf.putDouble(Double.MAX_VALUE);
    buf.putDouble(Double.MIN_VALUE);

    buf.resetPosition();

    assertEquals(buf.getBoolean(buf.getPosition()), true);
    assertEquals(buf.getBoolean(), true);
    assertEquals(buf.getBoolean(buf.getPosition()), false);
    assertEquals(buf.getBoolean(), false);
    assertEquals(buf.getByte(buf.getPosition()), (byte) -1);
    assertEquals(buf.getByte(), (byte) -1);
    assertEquals(buf.getByte(buf.getPosition()), (byte) 0);
    assertEquals(buf.getByte(), (byte) 0);
    assertEquals(buf.getChar(buf.getPosition()), 'A');
    assertEquals(buf.getChar(), 'A');
    assertEquals(buf.getChar(buf.getPosition()), 'Z');
    assertEquals(buf.getChar(), 'Z');
    assertEquals(buf.getShort(buf.getPosition()), Short.MAX_VALUE);
    assertEquals(buf.getShort(), Short.MAX_VALUE);
    assertEquals(buf.getShort(buf.getPosition()), Short.MIN_VALUE);
    assertEquals(buf.getShort(), Short.MIN_VALUE);
    assertEquals(buf.getInt(buf.getPosition()), Integer.MAX_VALUE);
    assertEquals(buf.getInt(), Integer.MAX_VALUE);
    assertEquals(buf.getInt(buf.getPosition()), Integer.MIN_VALUE);
    assertEquals(buf.getInt(), Integer.MIN_VALUE);
    assertEquals(buf.getFloat(buf.getPosition()), Float.MAX_VALUE, 0);
    assertEquals(buf.getFloat(), Float.MAX_VALUE, 0);
    assertEquals(buf.getFloat(buf.getPosition()), Float.MIN_VALUE, 0);
    assertEquals(buf.getFloat(), Float.MIN_VALUE, 0);
    assertEquals(buf.getLong(buf.getPosition()), Long.MAX_VALUE);
    assertEquals(buf.getLong(), Long.MAX_VALUE);
    assertEquals(buf.getLong(buf.getPosition()), Long.MIN_VALUE);
    assertEquals(buf.getLong(), Long.MIN_VALUE);
    assertEquals(buf.getDouble(buf.getPosition()), Double.MAX_VALUE, 0);
    assertEquals(buf.getDouble(), Double.MAX_VALUE, 0);
    assertEquals(buf.getDouble(buf.getPosition()), Double.MIN_VALUE, 0);
    assertEquals(buf.getDouble(), Double.MIN_VALUE, 0);
  }

  public static void setGetTests2(WritableBuffer buf) {
    buf.putBoolean(0, true);
    buf.putBoolean(1, false);
    buf.putByte(2, (byte) -1);
    buf.putByte(3, (byte) 0);
    buf.putChar(4, 'A');
    buf.putChar(6, 'Z');
    buf.putShort(8, Short.MAX_VALUE);
    buf.putShort(10, Short.MIN_VALUE);
    buf.putInt(12, Integer.MAX_VALUE);
    buf.putInt(16, Integer.MIN_VALUE);
    buf.putFloat(20, Float.MAX_VALUE);
    buf.putFloat(24, Float.MIN_VALUE);
    buf.putLong(28, Long.MAX_VALUE);
    buf.putLong(36, Long.MIN_VALUE);
    buf.putDouble(44, Double.MAX_VALUE);
    buf.putDouble(52, Double.MIN_VALUE);

    assertEquals(buf.getBoolean(0), true);
    assertEquals(buf.getBoolean(1), false);
    assertEquals(buf.getByte(2), (byte) -1);
    assertEquals(buf.getByte(3), (byte) 0);
    assertEquals(buf.getChar(4), 'A');
    assertEquals(buf.getChar(6), 'Z');
    assertEquals(buf.getShort(8), Short.MAX_VALUE);
    assertEquals(buf.getShort(10), Short.MIN_VALUE);
    assertEquals(buf.getInt(12), Integer.MAX_VALUE);
    assertEquals(buf.getInt(16), Integer.MIN_VALUE);
    assertEquals(buf.getFloat(20), Float.MAX_VALUE, 0);
    assertEquals(buf.getFloat(24), Float.MIN_VALUE, 0);
    assertEquals(buf.getLong(28), Long.MAX_VALUE);
    assertEquals(buf.getLong(36), Long.MIN_VALUE);
    assertEquals(buf.getDouble(44), Double.MAX_VALUE, 0);
    assertEquals(buf.getDouble(52), Double.MIN_VALUE, 0);
  }

  public static void setGetArraysTests(WritableBuffer buf) {
    int words = 4;

    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[words];
    buf.resetPosition();
    buf.fill((byte) 127);
    buf.resetPosition();
    buf.putBooleanArray(srcArray1, 0, words);
    buf.resetPosition();
    buf.getBooleanArray(dstArray1, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = {1, -2, 3, -4};
    byte[] dstArray2 = new byte[4];
    buf.resetPosition();
    buf.putByteArray(srcArray2, 0, words);
    buf.resetPosition();
    buf.getByteArray(dstArray2, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }

    char[] srcArray3 = {'A', 'B', 'C', 'D'};
    char[] dstArray3 = new char[words];
    buf.resetPosition();
    buf.putCharArray(srcArray3, 0, words);
    buf.resetPosition();
    buf.getCharArray(dstArray3, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }

    double[] srcArray4 = {1.0, -2.0, 3.0, -4.0};
    double[] dstArray4 = new double[words];
    buf.resetPosition();
    buf.putDoubleArray(srcArray4, 0, words);
    buf.resetPosition();
    buf.getDoubleArray(dstArray4, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }

    float[] srcArray5 = {(float) 1.0, (float) -2.0, (float) 3.0, (float) -4.0};
    float[] dstArray5 = new float[words];
    buf.resetPosition();
    buf.putFloatArray(srcArray5, 0, words);
    buf.resetPosition();
    buf.getFloatArray(dstArray5, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }

    int[] srcArray6 = {1, -2, 3, -4};
    int[] dstArray6 = new int[words];
    buf.resetPosition();
    buf.putIntArray(srcArray6, 0, words);
    buf.resetPosition();
    buf.getIntArray(dstArray6, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }

    long[] srcArray7 = {1, -2, 3, -4};
    long[] dstArray7 = new long[words];
    buf.resetPosition();
    buf.putLongArray(srcArray7, 0, words);
    buf.resetPosition();
    buf.getLongArray(dstArray7, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }

    short[] srcArray8 = {1, -2, 3, -4};
    short[] dstArray8 = new short[words];
    buf.resetPosition();
    buf.putShortArray(srcArray8, 0, words);
    buf.resetPosition();
    buf.getShortArray(dstArray8, 0, words);
    for (int i = 0; i < words; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }

  public static void setGetPartialArraysWithOffsetTests(WritableBuffer buf) {
    int items = 4;
    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[items];
    buf.resetPosition();
    buf.putBooleanArray(srcArray1, 2, items / 2);
    buf.resetPosition();
    buf.getBooleanArray(dstArray1, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = {1, -2, 3, -4};
    byte[] dstArray2 = new byte[items];
    buf.resetPosition();
    buf.putByteArray(srcArray2, 2, items / 2);
    buf.resetPosition();
    buf.getByteArray(dstArray2, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }

    char[] srcArray3 = {'A', 'B', 'C', 'D'};
    char[] dstArray3 = new char[items];
    buf.resetPosition();
    buf.putCharArray(srcArray3, 2, items / 2);
    buf.resetPosition();
    buf.getCharArray(dstArray3, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }

    double[] srcArray4 = {1.0, -2.0, 3.0, -4.0};
    double[] dstArray4 = new double[items];
    buf.resetPosition();
    buf.putDoubleArray(srcArray4, 2, items / 2);
    buf.resetPosition();
    buf.getDoubleArray(dstArray4, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }

    float[] srcArray5 = {(float) 1.0, (float) -2.0, (float) 3.0, (float) -4.0};
    float[] dstArray5 = new float[items];
    buf.resetPosition();
    buf.putFloatArray(srcArray5, 2, items / 2);
    buf.resetPosition();
    buf.getFloatArray(dstArray5, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }

    int[] srcArray6 = {1, -2, 3, -4};
    int[] dstArray6 = new int[items];
    buf.resetPosition();
    buf.putIntArray(srcArray6, 2, items / 2);
    buf.resetPosition();
    buf.getIntArray(dstArray6, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }

    long[] srcArray7 = {1, -2, 3, -4};
    long[] dstArray7 = new long[items];
    buf.resetPosition();
    buf.putLongArray(srcArray7, 2, items / 2);
    buf.resetPosition();
    buf.getLongArray(dstArray7, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }

    short[] srcArray8 = {1, -2, 3, -4};
    short[] dstArray8 = new short[items];
    buf.resetPosition();
    buf.putShortArray(srcArray8, 2, items / 2);
    buf.resetPosition();
    buf.getShortArray(dstArray8, 2, items / 2);
    for (int i = 2; i < items; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }

  //enable println statements to visually check
  public static void setClearMemoryRegionsTests(WritableBuffer buf) {
    int accessCapacity = (int) buf.getCapacity();

    //define regions
    int reg1Start = 0;
    int reg1Len = 28;
    int reg2Start = 28;
    int reg2Len = 32;

    //set region 1
    byte b1 = 5;
    buf.setStartPositionEnd(reg1Start, reg1Start, reg1Len);
    buf.fill(b1);
    buf.resetPosition();
    for (int i = reg1Start; i < (reg1Len + reg1Start); i++) {
      assertEquals(buf.getByte(), b1);
    }

    //set region 2
    byte b2 = 7;
    buf.setStartPositionEnd(reg2Start, reg2Start, reg2Start + reg2Len);
    buf.fill(b2);
    buf.resetPosition();
    for (int i = reg2Start; i < (reg2Start + reg2Len); i++) {
      assertEquals(buf.getByte(), b2);
    }

    //clear region 1
    byte zeroByte = 0;
    buf.setStartPositionEnd(reg1Start, reg1Start, reg2Len);
    buf.resetPosition();
    buf.clear();
    buf.resetPosition();
    for (int i = reg1Start; i < (reg1Start + reg1Len); i++) {
      assertEquals(buf.getByte(), zeroByte);
    }

    //clear region 2
    buf.setStartPositionEnd(reg2Start, reg2Start, reg2Start + reg2Len);
    buf.resetPosition();
    buf.clear();
    buf.resetPosition();
    for (int i = reg2Start; i < (reg2Len + reg2Start); i++) {
      assertEquals(buf.getByte(), zeroByte);
    }

    //set all to ones
    buf.setStartPositionEnd(reg1Start, reg1Start, accessCapacity);
    byte b4 = 127;
    buf.resetPosition();
    buf.fill(b4);
    buf.resetPosition();
    for (int i = 0; i < accessCapacity; i++) {
      assertEquals(buf.getByte(), b4);
    }

    //clear all
    buf.resetPosition();
    buf.clear();
    buf.resetPosition();
    for (int i = 0; i < accessCapacity; i++) {
      assertEquals(buf.getByte(), zeroByte);
    }
  }

}
