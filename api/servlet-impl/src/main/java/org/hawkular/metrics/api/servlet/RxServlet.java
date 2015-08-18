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
        AsyncContext asyncContext = getAsyncContext(req);

        // Write it to the outputStream (ignore errors for now)
        ObservableServlet.write(fakeDataSource, resp.getOutputStream())
                .subscribe(v -> {},
                        t -> {},
                        () -> {
                            resp.setStatus(HttpServletResponse.SC_OK);
                            asyncContext.complete();
                        });
    }

    private AsyncContext getAsyncContext(HttpServletRequest req) {
        if(req.isAsyncStarted()) {
            return req.getAsyncContext();
        } else {
            return req.startAsync();
        }
    }

    /**
     * From http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
     * @param v
     * @return
     */
    private int getNextPowerOf2(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        return ++v;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Fetch the input buffer
        // Lets gather up all the ByteBuffers for simpler prototype and transform to String..

        // We know each Observed ByteBuffer is going to be max. 4096 bytes
        int bufferSize = getNextPowerOf2(req.getContentLength());

        Observable<ByteBuffer> byteBufferObservable = ObservableServlet.create(req.getInputStream())
                .reduce(ByteBuffer.allocate(bufferSize), ByteBuffer::put);

        // Start the async, nothing is coming until we subscribe to byteBufferObservable
        final AsyncContext asyncContext = req.startAsync();

        if(req.getRequestURI().contains("return")) {

            // Our business logic
            Observable<ByteBuffer> fakeDataSource = byteBufferObservable
                    .map(bb -> {
                        StringBuilder sb = new StringBuilder();
                        sb.append("=== START OF THE PAYLOAD ===\n");
                        sb.append("InputFileSize: " + bb.remaining());
                        sb.append('\n')
                        sb.append("=== END OF THE PAYLOAD ===\n");
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
                    .subscribe(v -> {
                            },
                            t -> {
                            },
                            () -> {
                                resp.setStatus(HttpServletResponse.SC_CREATED);
                                asyncContext.complete();
                            });
        }
    }
}
