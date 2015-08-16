package org.hawkular.metrics.api.servlet;

import org.hawkular.metrics.api.servlet.rx.ObservableServlet;
import rx.Observable;
import rx.functions.Func2;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

/**
 * Created by miburman on 8/13/15.
 */
@WebServlet(asyncSupported = true, urlPatterns = "/rx/*")
public class RxServlet extends HttpServlet {

    // Emulate asynchronous fetching from a datasource and return using async I/O
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Emulate a data fetching from somewhere.. this isn't fetched until subscribe is called anyway
        Observable<ByteBuffer> fakeDataSource = Observable.just("Return string")
                .map(s -> ByteBuffer.wrap(s.getBytes()));

        // Start async part
        final AsyncContext asyncContext = req.startAsync();

        // Write it to the outputStream (ignore errors for now)
        ObservableServlet.write(fakeDataSource, resp.getOutputStream())
                .subscribe(v -> {},
                        t -> {},
                        () -> {
                            resp.setStatus(HttpServletResponse.SC_OK);
                            asyncContext.complete();
                        });
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Fetch the input buffer
        // Lets gather up all the ByteBuffers for simpler prototype and transform to String..
        Observable<ByteBuffer> byteBufferObservable = ObservableServlet.create(req.getInputStream())
                .reduce((byteBuffer, byteBuffer2) -> {
                    ByteBuffer backingBuffer;

                    if (byteBuffer2.remaining() > (byteBuffer.capacity() - byteBuffer.limit())) {
                        backingBuffer = ByteBuffer.allocate(byteBuffer.capacity() + byteBuffer2.remaining() * 2); // It's virtual memory anyway..
                        backingBuffer.put(byteBuffer).put(byteBuffer2);
                    } else {
                        backingBuffer = byteBuffer.put(byteBuffer2);
                    }

                    return backingBuffer;
                });

        // Start the async, nothing is coming until we subscribe to byteBufferObservable
        final AsyncContext asyncContext = req.startAsync();

        if(req.getRequestURI().contains("return")) {

            // Our business logic
            Observable<ByteBuffer> fakeDataSource = byteBufferObservable
                    .map(bb -> {
                        StringBuilder sb = new StringBuilder();
                        sb.append("=== START OF THE PAYLOAD ===");
                        sb.append("InputFileSize: " + bb.remaining());
                        sb.append("=== END OF THE PAYLOAD ===");
                        return sb.toString();
                    })
                    .map(s -> ByteBuffer.wrap(s.getBytes()));

            // Write to listener
            ObservableServlet.write(fakeDataSource, resp.getOutputStream())
                    .subscribe(v -> {},
                            t -> {},
                            () -> {
                                resp.setStatus(HttpServletResponse.SC_OK);
                                asyncContext.complete();
                            });
        } else {
            byteBufferObservable
                    .subscribe(v -> {},
                            t -> {},
                            () -> {
                                resp.setStatus(HttpServletResponse.SC_CREATED);
                                asyncContext.complete();
                            });
        }
    }
}
