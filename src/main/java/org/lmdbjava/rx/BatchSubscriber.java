package org.lmdbjava.rx;

import org.agrona.DirectBuffer;
import org.lmdbjava.*;
import rx.Subscriber;
import rx.exceptions.OnErrorFailedException;

import java.util.List;

class BatchSubscriber<T extends DirectBuffer> extends Subscriber<List<KeyVal<T>>> {
  final Dbi<T> db;
  final Cursor<T> cursor;
  final Txn<T> tx;
  final Env env;

  BatchSubscriber(Txn<T> tx, Dbi<T> db) {
    this.env = db.getEnv();
    this.tx = tx;
    this.db = db;
    this.cursor = db.openCursor(tx);
  }

  @Override
  public void onCompleted() {
  }

  @Override
  public void onError(Throwable e) {
    System.err.println("Batch error");
    e.printStackTrace(System.err);
  }

  @Override
  public void onNext(List<KeyVal<T>> kvs) {
    try {
      if (kvs.size() < 1) {
        return;
      }
      for (KeyVal<T> kv : kvs) {
        try {
          cursor.put(kv.key, kv.val);
        } catch (Throwable e) {
          // log error, swallow exception and proceed to next kv
          System.err.println("Batch put error.");
          e.printStackTrace(System.err);
        }
      }
    } catch (Throwable e) {
      throw new OnErrorFailedException(e);
    }
  }
}