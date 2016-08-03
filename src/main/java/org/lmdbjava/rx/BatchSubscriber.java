
package org.lmdbjava.rx;

import java.util.List;
import org.agrona.DirectBuffer;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import rx.Subscriber;
import rx.exceptions.OnErrorFailedException;

class BatchSubscriber<T extends DirectBuffer> extends Subscriber<List<KeyVal<T>>> {

  final Cursor<T> cursor;
  final Dbi<T> db;
  final Txn<T> tx;

  BatchSubscriber(Txn<T> tx, Dbi<T> db) {
    this.tx = tx;
    this.db = db;
    this.cursor = db.openCursor(tx);
  }

  @Override
  public void onCompleted() {
  }

  @Override
  public void onError(Throwable e) {
  }

  @Override
  public void onNext(List<KeyVal<T>> kvs) {
    try {
      if (kvs.size() < 1) {
        return;
      }
      for (KeyVal<T> kv : kvs) {
        try {
          cursor.put(kv.key(), kv.val());
        } catch (RuntimeException ignored) {
        }
      }
    } catch (Throwable e) {
      throw new OnErrorFailedException(e);
    }
  }
}
