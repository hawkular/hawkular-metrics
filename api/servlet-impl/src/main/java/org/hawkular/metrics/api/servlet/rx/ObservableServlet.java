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

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;

import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * An {@link Observable} interface to Servlet API
 *
 * @author Jitendra Kotamraju
 */
public class ObservableServlet {

    /**
     * Observes {@link ServletInputStream}.
     *
     * <p>
     * This method uses Servlet 3.1 non-blocking API callback mechanisms. When the HTTP
     * request data becomes available to be read, the subscribed {@code Observer}'s
     * {@link Observer#onNext onNext} method is invoked. Similarly, when all data for the
     * HTTP request has been read, the subscribed {@code Observer}'s
     * {@link Observer#onCompleted onCompleted} method is invoked.
     *
     * <p>
     * Before calling this method, a web application must put the corresponding HTTP request
     * into asynchronous mode.
     *
     * @param in servlet input stream
     * @return Observable of HTTP request data
     */
    public static Observable<ByteBuffer> create(final ServletInputStream in) {
        return Observable.create(new OnSubscribe<ByteBuffer>() {
            @Override
            public void call(Subscriber<? super ByteBuffer> subscriber) {
                final ServletReadListener listener = new ServletReadListener(in, subscriber);
                in.setReadListener(listener);
            }
        });
    }

    /**
     * Observes {@link ServletOutputStream}.
     *
     * <p>
     * This method uses Servlet 3.1 non-blocking API callback mechanisms. When the
     * container notifies that HTTP response can be written, the subscribed
     * {@code Observer}'s {@link Observer#onNext onNext} method is invoked.
     *
     * <p>
     * Before calling this method, a web application must put the corresponding HTTP
     * request into asynchronous mode.
     *
     * @param out servlet output stream
     * @return Observable of HTTP response write ready events
     */
    public static Observable<Void> create(final ServletOutputStream out) {

        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> subscriber) {
                final ServletWriteListener listener = new ServletWriteListener(subscriber, out);
                out.setWriteListener(listener);
            }
        });
    }

    /**
     * Writes the given Observable data to ServletOutputStream.
     *
     * <p>
     * This method uses Servlet 3.1 non-blocking API callback mechanisms. When the HTTP
     * request data becomes available to be read, the subscribed {@code Observer}'s
     * {@link Observer#onNext onNext} method is invoked. Similarly, when all data for the
     * HTTP request has been read, the subscribed {@code Observer}'s
     * {@link Observer#onCompleted onCompleted} method is invoked.
     *
     * <p>
     * Before calling this method, a web application must put the corresponding HTTP request
     * into asynchronous mode.
     *
     * @param data
     * @param out servlet output stream
     * @return
     */
    public static Observable<Void> write(final Observable<ByteBuffer> data, final ServletOutputStream out) {
        return Observable.create(new OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> subscriber) {
                Observable<Void> events = create(out);
                Observable<Void> writeobs = Observable.zip(data, events, (byteBuffer, aVoid) -> {
                    try {
                        byte[] b = new byte[byteBuffer.remaining()];
                        byteBuffer.get(b);
                        out.write(b);
                    } catch (IOException ioe) {
                        ioe.printStackTrace();
                    }
                    return null;
                });
                writeobs.subscribe(subscriber);
            }
        });
    }

}