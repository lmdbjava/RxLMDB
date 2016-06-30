package org.lmdbjava.rx;

import org.agrona.MutableDirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteOrder;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.MutableDirectBufferProxy.PROXY_MDB;
import static org.lmdbjava.rx.TestUtil.mdb;

public class ScanTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env<MutableDirectBuffer> env;
  private Dbi<MutableDirectBuffer> db;

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
  public void forwardScan() {
    try(Txn<MutableDirectBuffer> tx = env.txnRead()) {
      List<KeyVal<MutableDirectBuffer>> list = RxLMDB.scan(tx, db)
        .toList().toBlocking().first();
      assertThat(list.size(), is(5));
      assertThat(list.get(0).key.getInt(0), is(1));
      assertThat(list.get(0).val.getInt(0), is(2));
      assertThat(list.get(1).key.getInt(0), is(3));
      assertThat(list.get(1).val.getInt(0), is(4));
      assertThat(list.get(2).key.getInt(0), is(5));
      assertThat(list.get(2).val.getInt(0), is(6));
      assertThat(list.get(3).key.getInt(0), is(7));
      assertThat(list.get(3).val.getInt(0), is(8));
      assertThat(list.get(4).key.getInt(0), is(9));
      assertThat(list.get(4).val.getInt(0), is(10));
    }
  }
}
