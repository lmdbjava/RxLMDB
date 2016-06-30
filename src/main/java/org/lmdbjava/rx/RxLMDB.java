package org.lmdbjava.rx;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.*;
import rx.Observable;

import java.util.function.Function;

public class RxLMDB {

  public static Observable<KeyVal<DirectBuffer>> scanForward(
    Txn<MutableDirectBuffer> tx, Dbi<MutableDirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.first(), cursor -> cursor.next());
  }

  public static Observable<KeyVal<DirectBuffer>> scanBackward(
    Txn<MutableDirectBuffer> tx, Dbi<MutableDirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.last(), cursor -> cursor.prev());
  }

  private static Observable<KeyVal<DirectBuffer>> scan(
    Txn<MutableDirectBuffer> tx,
    Dbi<MutableDirectBuffer> db,
    Function<Cursor<MutableDirectBuffer>, Boolean> first,
    Function<Cursor<MutableDirectBuffer>, Boolean> next) {
    Cursor<MutableDirectBuffer> cursor = db.openCursor(tx);
    return Observable.create(subscriber -> {
      try {
        boolean hasNext = first.apply(cursor);
        while (hasNext) {
          if (subscriber.isUnsubscribed()) {
            return;
          }
          // important : buffer addresses are only valid within
          // lifetime of the transaction -> user must ensure this
          DirectBuffer key = new UnsafeBuffer(cursor.key());
          DirectBuffer val = new UnsafeBuffer(cursor.val());
          subscriber.onNext(new KeyVal<>(key, val));
          hasNext = next.apply(cursor);
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
