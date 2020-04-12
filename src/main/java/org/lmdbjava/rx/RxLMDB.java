/*-
 * #%L
 * RxLMDB
 * %%
 * Copyright (C) 2016 - 2020 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

/**
 * Main class.
 */
@SuppressWarnings("checkstyle:abbreviationaswordinname")
public final class RxLMDB {

  private RxLMDB() {
  }

  public static <T extends DirectBuffer> void batch(
      final Txn<T> tx,
      final Dbi<T> db,
      final Observable<List<KeyVal<T>>> values) {
    final BatchSubscriber<T> putSubscriber = new BatchSubscriber<>(tx, db);
    values.subscribe(putSubscriber);
  }

  public static Observable<KeyVal<DirectBuffer>> get(
      final Txn<DirectBuffer> tx,
      final Dbi<DirectBuffer> db,
      final Observable<DirectBuffer> keys) {
    final Cursor<DirectBuffer> cursor = db.openCursor(tx);
    return create(subscriber -> {
      keys.subscribe(new SubscriberImpl(subscriber, cursor));
    });
  }

  public static Observable<KeyVal<DirectBuffer>> scanBackward(
      final Txn<DirectBuffer> tx,
      final Dbi<DirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.last(), cursor -> cursor.prev());
  }

  public static Observable<KeyVal<DirectBuffer>> scanForward(
      final Txn<DirectBuffer> tx,
      final Dbi<DirectBuffer> db) {
    return scan(tx, db, cursor -> cursor.first(), cursor -> cursor.next());
  }

  @SuppressWarnings({"PMD.AvoidInstantiatingObjectsInLoops",
                     "PMD.AvoidCatchingThrowable"})
  private static Observable<KeyVal<DirectBuffer>> scan(
      final Txn<DirectBuffer> tx,
      final Dbi<DirectBuffer> db,
      final Function<Cursor<DirectBuffer>, Boolean> first,
      final Function<Cursor<DirectBuffer>, Boolean> next) {
    final Cursor<DirectBuffer> cursor = db.openCursor(tx);
    return create(subscriber -> {
      try {
        boolean hasNext = first.apply(cursor);
        while (hasNext) {
          if (subscriber.isUnsubscribed()) {
            return;
          }
          // important : buffer addresses are only valid within
          // lifetime of the transaction -> user must ensure this
          final DirectBuffer key = new UnsafeBuffer(cursor.key());
          final DirectBuffer val = new UnsafeBuffer(cursor.val());
          subscriber.onNext(new KeyVal<>(key, val));
          hasNext = next.apply(cursor);
        }

        if (!subscriber.isUnsubscribed()) {
          subscriber.onCompleted();
        }
      } catch (final Throwable e) {
        if (cursor != null) {
          cursor.close();
        }
        tx.abort();
        subscriber.onError(e);
      } finally {
        if (cursor != null) {
          cursor.close();
        }
        // no op if tx was aborted
        tx.commit();
      }
    });
  }

  /**
   * Subscriber.
   */
  private static class SubscriberImpl extends Subscriber<DirectBuffer> {

    private final Cursor<DirectBuffer> cursor;
    private final Subscriber<? super KeyVal<DirectBuffer>> subscriber;

    SubscriberImpl(
        final Subscriber<? super KeyVal<DirectBuffer>> subscriber,
        final Cursor<DirectBuffer> cursor) {
      super();
      this.subscriber = subscriber;
      this.cursor = cursor;
    }

    @Override
    public void onCompleted() {
      subscriber.onCompleted();
    }

    @Override
    public void onError(final Throwable throwable) {
      subscriber.onError(throwable);
    }

    @Override
    public void onNext(final DirectBuffer key) {
      if (cursor.get(key, MDB_SET)) {
        // important : buffer addresses are only valid within
        // lifetime of the transaction -> user must ensure this
        final DirectBuffer k = new UnsafeBuffer(cursor.key());
        final DirectBuffer v = new UnsafeBuffer(cursor.val());
        subscriber.onNext(new KeyVal<>(k, v));
      }
    }
  }

}
