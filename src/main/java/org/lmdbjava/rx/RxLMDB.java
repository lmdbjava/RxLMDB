package org.lmdbjava.rx;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.*;
import rx.Observable;

public class RxLMDB {

  public static Observable<KeyVal<MutableDirectBuffer>> scan(
    Txn<MutableDirectBuffer> tx, Dbi<MutableDirectBuffer> db) {
    Cursor<MutableDirectBuffer> cursor = db.openCursor(tx);
    return Observable.create(subscriber -> {
      try {
        boolean hasNext = cursor.first();
        while (hasNext) {
          if (subscriber.isUnsubscribed()) {
            return;
          }
          MutableDirectBuffer key = new UnsafeBuffer(cursor.key());
          MutableDirectBuffer val = new UnsafeBuffer(cursor.val());
          subscriber.onNext(new KeyVal<>(key, val));
          hasNext = cursor.next();
        }

        if (!subscriber.isUnsubscribed()) {
          subscriber.onCompleted();
        }
      } catch (Throwable e) {
        if (cursor != null) {
          cursor.close();
        }
        tx.abort();
        subscriber.onError(e);
      } finally {
        cursor.close();
        // no op if tx was aborted
        tx.commit();
      }
    });
  }
}
