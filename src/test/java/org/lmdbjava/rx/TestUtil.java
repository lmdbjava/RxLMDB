
package org.lmdbjava.rx;

import static java.lang.Long.BYTES;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.CursorIterator.KeyVal;

public class TestUtil {

  private TestUtil() {
  }

  static KeyVal<DirectBuffer> kv(int k, int v) {
    return new KeyVal<>(mdb(k), mdb(v));
  }

  static MutableDirectBuffer mdb(final int value) {
    ByteBuffer byteBuffer = allocateDirect(BYTES);
    final MutableDirectBuffer b = new UnsafeBuffer(byteBuffer);
    b.putInt(0, value, LITTLE_ENDIAN);
    return b;
  }

}
