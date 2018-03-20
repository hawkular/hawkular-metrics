/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.api.servlet.rx;

import java.io.IOException;

import javax.servlet.ServletOutputStream;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

/**
 * An {@link Observable} interface to Servlet API
 *
 * @author Jitendra Kotamraju
 * @author Michael Burman
 */
public class ObservableServlet {

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
        return Observable.unsafeCreate(subscriber -> {
            final ServletWriteListener listener = new ServletWriteListener(subscriber, out);
            out.setWriteListener(listener);
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
    public static Observable<Void> write(final Observable<byte[]> data, final ServletOutputStream out) {
        return Observable.create(new Observable.OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> subscriber) {
                Observable<Void> events = create(out).onBackpressureDrop();
                Observable<Void> writeobs = Observable.zip(data, events, (b, aVoid) -> {
                    try {
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
