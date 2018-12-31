/*-
 * #%L
 * RxLMDB
 * %%
 * Copyright (C) 2016 - 2019 The LmdbJava Open Source Project
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
import org.agrona.DirectBuffer;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterator.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;
import rx.Subscriber;
import rx.exceptions.OnErrorFailedException;

/**
 * Batch subscriber.
 *
 * @param <T> buffer type
 */
final class BatchSubscriber<T extends DirectBuffer> extends Subscriber<List<KeyVal<T>>> {

  private final Cursor<T> cursor;

  BatchSubscriber(final Txn<T> tx, final Dbi<T> db) {
    super();
    this.cursor = db.openCursor(tx);
  }

  @Override
  public void onCompleted() {
  }

  @Override
  public void onError(final Throwable e) {
  }

  @Override
  @SuppressWarnings({"PMD.AvoidCatchingGenericException",
                     "PMD.AvoidCatchingThrowable"})
  public void onNext(final List<KeyVal<T>> kvs) {
    try {
      if (kvs.isEmpty()) {
        return;
      }
      for (final KeyVal<T> kv : kvs) {
        try {
          cursor.put(kv.key(), kv.val());
        } catch (final RuntimeException ignored) {
        }
      }
    } catch (final Throwable e) {
      throw new OnErrorFailedException(e);
    }
  }
}
