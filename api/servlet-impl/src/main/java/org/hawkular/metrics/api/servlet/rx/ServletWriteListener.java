package org.hawkular.metrics.api.servlet.rx;

/**
 * Copyright 2014 Jitendra Kotamraju.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import rx.Subscriber;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

/**
 * A servlet {@link WriteListener} that pushes Observable events that indicate
 * when HTTP response data can be written
 *
 * @author Jitendra Kotamraju
 */
class ServletWriteListener implements WriteListener {

    private final Subscriber<? super Void> subscriber;
    private final ServletOutputStream out;

    ServletWriteListener(Subscriber<? super Void> subscriber, final ServletOutputStream out) {
        this.subscriber = subscriber;
        this.out = out;
    }

    @Override
    public void onWritePossible() {
        do {
            subscriber.onNext(null);
            // loop until isReady() false, otherwise container will not call onWritePossible()
        } while (!subscriber.isUnsubscribed() && out.isReady());
        // If isReady() false, container will call onWritePossible()
        // when data can be written.
    }

    @Override
    public void onError(Throwable t) {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onError(t);
        }
    }

}