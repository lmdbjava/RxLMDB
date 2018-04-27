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

import java.io.File;
import java.io.IOException;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import static org.lmdbjava.rx.RxLMDB.scanForward;
import static org.lmdbjava.rx.TestUtil.createEnv;
import static org.lmdbjava.rx.TestUtil.kv;
import static org.lmdbjava.rx.TestUtil.verifyList;
import static rx.Observable.from;

@SuppressWarnings("checkstyle:javadoctype")
public final class PutTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<DirectBuffer> db;

  private Env<DirectBuffer> env;

  @Test
  public void batch() throws InterruptedException {
    final List<List<KeyVal<DirectBuffer>>> kvs = asList(asList(
        kv(1, 2),
        kv(3, 4)));

    try (Txn<DirectBuffer> tx = env.txnWrite()) {
      RxLMDB.batch(tx, db, from(kvs));
      sleep(100);
      tx.commit();
    }

    try (Txn<DirectBuffer> tx = env.txnRead()) {
      final List<KeyVal<DirectBuffer>> list = scanForward(tx, db)
          .toList().toBlocking().first();
      verifyList(list, 2);
    }
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = createEnv(path);
    db = env.openDbi("test", MDB_CREATE);
  }
}
