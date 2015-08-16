package org.hawkular.metrics.api.servlet.rx;

/**
 * Copyright 2013-2014 Jitendra Kotamraju.
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

import rx.Observer;
import rx.Subscriber;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A servlet {@link ReadListener} that pushes HTTP request data to an {@link Observer}
 *
 * @author Jitendra Kotamraju
 * @author miburman
 */
class ServletReadListener implements ReadListener {
    private final Subscriber<? super ByteBuffer> subscriber;
    private final ServletInputStream in;

    ServletReadListener(ServletInputStream in, Subscriber<? super ByteBuffer> subscriber) {
        this.in = in;
        this.subscriber = subscriber;
    }

    @Override
    public void onDataAvailable() throws IOException {
        // Original RxServlet has a bug here, in.isFinished() needs to be called, as
        // in.isReady() returns true on EOF
        while(in.isReady() && !in.isFinished() && !subscriber.isUnsubscribed()) {
            byte[] buf = new byte[4096];
            int len = in.read(buf);
            if (len != -1) {
                subscriber.onNext(ByteBuffer.wrap(buf, 0, len));
            }
        }
    }

    @Override
    public void onAllDataRead() {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onCompleted();
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!subscriber.isUnsubscribed()) {
            subscriber.onError(t);
        }
    }

}