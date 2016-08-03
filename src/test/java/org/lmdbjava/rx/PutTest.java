
package org.lmdbjava.rx;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import java.io.File;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
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
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import org.lmdbjava.Env;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import org.lmdbjava.Txn;
import static org.lmdbjava.rx.RxLMDB.scanForward;
import static org.lmdbjava.rx.TestUtil.kv;
import static rx.Observable.from;

public class PutTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<DirectBuffer> db;

  private Env<DirectBuffer> env;

  @Test
  public void batch() throws InterruptedException {
    List<List<KeyVal<DirectBuffer>>> kvs = asList(asList(
        kv(1, 2),
        kv(3, 4)));

    try (Txn<DirectBuffer> tx = env.txnWrite()) {
      RxLMDB.batch(tx, db, from(kvs));
      sleep(100);
      tx.commit();
    }

    try (Txn<DirectBuffer> tx = env.txnRead()) {
      List<KeyVal<DirectBuffer>> list = scanForward(tx, db)
          .toList().toBlocking().first();
      assertThat(list.get(0).key().getInt(0), is(1));
      assertThat(list.get(0).val().getInt(0), is(2));
      assertThat(list.get(1).key().getInt(0), is(3));
      assertThat(list.get(1).val().getInt(0), is(4));
    }
  }

  @Before
  public void before() throws Exception {
    final File path = tmp.newFile();
    env
        = create(PROXY_DB).setMapSize(MEBIBYTES.toBytes(1)).setMaxReaders(1)
        .setMaxDbs(2).open(path, MDB_NOSUBDIR);
    db = env.openDbi("test", MDB_CREATE);
  }
}
