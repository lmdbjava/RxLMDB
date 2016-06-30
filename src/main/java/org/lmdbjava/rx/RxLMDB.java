package org.lmdbjava.rx;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.lmdbjava.*;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.OnErrorFailedException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

public class RxLMDB {

  public static Observable<KeyVal<DirectBuffer>> scanForward(
    Txn<MutableDirectBuffer> tx,
    Dbi<MutableDirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.first(), cursor -> cursor.next());
  }

  public static Observable<KeyVal<DirectBuffer>> scanBackward(
    Txn<MutableDirectBuffer> tx,
    Dbi<MutableDirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.last(), cursor -> cursor.prev());
  }

  public static <T extends DirectBuffer> void batch(
    Txn<MutableDirectBuffer> tx,
    Dbi<MutableDirectBuffer> db,
    Observable<List<KeyVal<T>>> values) {
    BatchSubscriber putSubscriber = new BatchSubscriber(tx, db);
    values.subscribe(putSubscriber);
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


  private static class BatchSubscriber<T extends DirectBuffer> extends Subscriber<List<KeyVal<T>>> {
    final Dbi<T> db;
    final Cursor<T> cursor;
    final Txn<T> tx;
    final Env env;

    private BatchSubscriber(Txn<T> tx, Dbi<T> db) {
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
}
