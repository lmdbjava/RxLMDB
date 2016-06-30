package org.lmdbjava.rx;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
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
import static org.lmdbjava.DirectBufferProxy.PROXY_MDB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.rx.TestUtil.mdb;

public class GetTest {
  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<DirectBuffer> env;
  private Dbi<DirectBuffer> db;

  @Before
  public void before() throws Exception {
    final File path = tmp.newFile();
    env = create(PROXY_MDB)
      .setMapSize(1, ByteUnit.MEBIBYTES)
      .setMaxReaders(1)
      .setMaxDbs(2)
      .open(path, MDB_NOSUBDIR);
    db = env.openDbi("test", DbiFlags.MDB_CREATE);

    db.put(mdb(1), mdb(2));
    db.put(mdb(3), mdb(4));
    db.put(mdb(5), mdb(6));
    db.put(mdb(7), mdb(8));
    db.put(mdb(9), mdb(10));
  }


  @Test
  public void getAll() {
    try(Txn<DirectBuffer> tx = env.txnRead()) {
      Observable<DirectBuffer> keys = Observable.from(Arrays.asList(mdb(1), mdb(3), mdb(7)));
      List<KeyVal<DirectBuffer>> list = RxLMDB.get(tx, db, keys)
        .toList().toBlocking().first();
      assertThat(list.size(), is(3));
      assertThat(list.get(0).key.getInt(0), is(1));
      assertThat(list.get(0).val.getInt(0), is(2));
      assertThat(list.get(1).key.getInt(0), is(3));
      assertThat(list.get(1).val.getInt(0), is(4));
      assertThat(list.get(2).key.getInt(0), is(7));
      assertThat(list.get(2).val.getInt(0), is(8));
    }
  }
}
