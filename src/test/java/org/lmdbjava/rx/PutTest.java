package org.lmdbjava.rx;

import com.jakewharton.byteunits.BinaryByteUnit;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.*;
import rx.Observable;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.lmdbjava.CursorIterator.KeyVal;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.rx.TestUtil.kv;

public class PutTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<DirectBuffer> env;
  private Dbi<DirectBuffer> db;

  @Before
  public void before() throws Exception {
    final File path = tmp.newFile();
    env = create(PROXY_DB)
      .setMapSize(BinaryByteUnit.MEBIBYTES.toBytes(1))
      .setMaxReaders(1)
      .setMaxDbs(2)
      .open(path, MDB_NOSUBDIR);
    db = env.openDbi("test", DbiFlags.MDB_CREATE);
  }

  @Test
  public void batch() throws InterruptedException {
    List<List<KeyVal<DirectBuffer>>> kvs =
      Arrays.asList(
        Arrays.asList(
          kv(1, 2),
          kv(3, 4)));

    try (Txn<DirectBuffer> tx = env.txnWrite()) {
      RxLMDB.batch(tx, db, Observable.from(kvs));
      Thread.sleep(100);
      tx.commit();
    }

    try (Txn<DirectBuffer> tx = env.txnRead()) {
      List<KeyVal<DirectBuffer>> list = RxLMDB.scanForward(tx, db)
        .toList().toBlocking().first();
      assertThat(list.get(0).key().getInt(0), is(1));
      assertThat(list.get(0).val().getInt(0), is(2));
      assertThat(list.get(1).key().getInt(0), is(3));
      assertThat(list.get(1).val().getInt(0), is(4));
    }
  }
}
