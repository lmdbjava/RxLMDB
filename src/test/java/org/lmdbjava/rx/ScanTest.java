/*-
 * #%L
 * RxLMDB
 * %%
 * Copyright (C) 2016 The LmdbJava Open Source Project
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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.agrona.DirectBuffer;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import static org.lmdbjava.rx.RxLMDB.scanBackward;
import static org.lmdbjava.rx.RxLMDB.scanForward;
import static org.lmdbjava.rx.TestUtil.createDbi;
import static org.lmdbjava.rx.TestUtil.createEnv;
import static org.lmdbjava.rx.TestUtil.verifyList;

@SuppressWarnings("checkstyle:javadoctype")
public final class ScanTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<DirectBuffer> db;
  private Env<DirectBuffer> env;

  @Test
  public void backwardScan() {
    try (final Txn<DirectBuffer> tx = env.txnRead()) {
      final List<KeyVal<DirectBuffer>> list
          = scanBackward(tx, db).toList().toBlocking().first();
      assertThat(list.size(), is(5));
      assertThat(list.get(4).key().getInt(0), is(1));
      assertThat(list.get(4).val().getInt(0), is(2));
      assertThat(list.get(3).key().getInt(0), is(3));
      assertThat(list.get(3).val().getInt(0), is(4));
      assertThat(list.get(2).key().getInt(0), is(5));
      assertThat(list.get(2).val().getInt(0), is(6));
      assertThat(list.get(1).key().getInt(0), is(7));
      assertThat(list.get(1).val().getInt(0), is(8));
      assertThat(list.get(0).key().getInt(0), is(9));
      assertThat(list.get(0).val().getInt(0), is(10));
    }
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = createEnv(path);
    db = createDbi(env);
  }

  @Test
  public void forwardScan() {
    try (final Txn<DirectBuffer> tx = env.txnRead()) {
      final List<KeyVal<DirectBuffer>> list = scanForward(tx, db)
          .toList().toBlocking().first();
      verifyList(list, 5);
    }
  }

}
