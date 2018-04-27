/*-
 * #%L
 * RxLMDB
 * %%
 * Copyright (C) 2016 - 2018 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava.rx;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import java.io.File;
import java.io.IOException;
import static java.lang.Long.BYTES;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import org.lmdbjava.Env;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;

/**
 * Utility methods for use in the test package.
 */
final class TestUtil {

  private TestUtil() {
  }

  static Dbi<DirectBuffer> createDbi(final Env<DirectBuffer> env) {
    final Dbi<DirectBuffer> db = env.openDbi("test", MDB_CREATE);
    db.put(mdb(1), mdb(2));
    db.put(mdb(3), mdb(4));
    db.put(mdb(5), mdb(6));
    db.put(mdb(7), mdb(8));
    db.put(mdb(9), mdb(10));
    return db;
  }

  static Env<DirectBuffer> createEnv(final File path) throws IOException {
    return create(PROXY_DB)
        .setMapSize(MEBIBYTES.toBytes(1)).setMaxReaders(1)
        .setMaxDbs(2).open(path, MDB_NOSUBDIR);
  }

  static KeyVal<DirectBuffer> kv(final int k, final int v) {
    return new KeyVal<>(mdb(k), mdb(v));
  }

  static MutableDirectBuffer mdb(final int value) {
    final ByteBuffer byteBuffer = allocateDirect(BYTES);
    final MutableDirectBuffer b = new UnsafeBuffer(byteBuffer);
    b.putInt(0, value, LITTLE_ENDIAN);
    return b;
  }

  /**
   * Verifies the presented list contains at least the specified number of
   * elements and the list commences with those consecutive elements.
   *
   * @param list             to check
   * @param expectedElements number of consecutive elements the list must have
   */
  static void verifyList(final List<KeyVal<DirectBuffer>> list,
                         final int expectedElements) {
    assertThat(list.size(), greaterThanOrEqualTo(expectedElements));
    for (int elementIndex = 0; elementIndex < expectedElements; elementIndex++) {
      final int expectKey = elementIndex * 2 + 1;
      final int expectVal = expectKey + 1;
      assertThat(list.get(elementIndex).key().getInt(0), is(expectKey));
      assertThat(list.get(elementIndex).val().getInt(0), is(expectVal));
    }
  }

}
