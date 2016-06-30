package org.lmdbjava.rx;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static java.lang.Integer.BYTES;
import static java.nio.ByteBuffer.allocateDirect;

public class TestUtil {
  static MutableDirectBuffer mdb(final int value) {
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(Long.BYTES);
    final MutableDirectBuffer b = new UnsafeBuffer(byteBuffer);
    b.putInt(0, value, ByteOrder.LITTLE_ENDIAN);
    return b;
  }
}
