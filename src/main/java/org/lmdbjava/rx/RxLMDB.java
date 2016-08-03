
package org.lmdbjava.rx;

import java.util.List;
import java.util.function.Function;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import static org.lmdbjava.GetOp.MDB_SET;
import org.lmdbjava.Txn;
import rx.Observable;
import static rx.Observable.create;
import rx.Subscriber;

public class RxLMDB {

  private RxLMDB() {
  }

  public static <T extends DirectBuffer> void batch(
      Txn<DirectBuffer> tx,
      Dbi<DirectBuffer> db,
      Observable<List<KeyVal<T>>> values) {
    BatchSubscriber putSubscriber = new BatchSubscriber(tx, db);
    values.subscribe(putSubscriber);
  }

  public static Observable<KeyVal<DirectBuffer>> get(
      Txn<DirectBuffer> tx,
      Dbi<DirectBuffer> db,
      Observable<DirectBuffer> keys) {
    Cursor<DirectBuffer> cursor = db.openCursor(tx);
    return create(subscriber -> {
      keys.subscribe(new Subscriber<DirectBuffer>() {
        @Override
        public void onCompleted() {
          subscriber.onCompleted();
        }

        @Override
        public void onError(Throwable throwable) {
          subscriber.onError(throwable);
        }

        @Override
        public void onNext(DirectBuffer key) {
          if (cursor.get(key, MDB_SET)) {
            // important : buffer addresses are only valid within
            // lifetime of the transaction -> user must ensure this
            DirectBuffer k = new UnsafeBuffer(cursor.key());
            DirectBuffer v = new UnsafeBuffer(cursor.val());
            subscriber.onNext(new KeyVal<>(k, v));
          } else {

          }
        }
      });
    });
  }

  public static Observable<KeyVal<DirectBuffer>> scanBackward(
      Txn<DirectBuffer> tx,
      Dbi<DirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.last(), cursor -> cursor.prev());
  }

  public static Observable<KeyVal<DirectBuffer>> scanForward(
      Txn<DirectBuffer> tx,
      Dbi<DirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.first(), cursor -> cursor.next());
  }

  private static Observable<KeyVal<DirectBuffer>> scan(
      Txn<DirectBuffer> tx,
      Dbi<DirectBuffer> db,
      Function<Cursor<DirectBuffer>, Boolean> first,
      Function<Cursor<DirectBuffer>, Boolean> next) {
    Cursor<DirectBuffer> cursor = db.openCursor(tx);
    return create(subscriber -> {
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
