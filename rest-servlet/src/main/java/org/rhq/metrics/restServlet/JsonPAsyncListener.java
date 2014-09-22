package org.rhq.metrics.restServlet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.ServletResponseWrapper;
import javax.servlet.http.HttpServletResponse;

/**
 * Listener that is called when the async result is available for the filter.
 * See {@link JsonPFilter} for where this is instantiated.
 * @author Heiko W. Rupp
 */
public class JsonPAsyncListener implements AsyncListener {

    private ServletResponseWrapper responseWrapper;
    private ServletResponse origResponse;
    private String callback;

    public JsonPAsyncListener() {
    }


    @Override
    public void onComplete(AsyncEvent asyncEvent) throws IOException {
        // Async processing is complete. We can now wrap the
        // returned output stream and set the content type.

        ServletResponse response = asyncEvent.getSuppliedResponse();

        ServletOutputStream outputStream = response.getOutputStream();
        origResponse.setContentType("application/javascript; charset=utf-8");
        origResponse.getOutputStream().write((callback + "(").getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream1 = ((JsonPFilter.JsonPResponseWrapper)responseWrapper).getByteArrayOutputStream();
        outputStream1.writeTo(origResponse.getOutputStream());
        origResponse.getOutputStream().write(");".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

    }

    @Override
    public void onTimeout(AsyncEvent asyncEvent) throws IOException {
    }

    @Override
    public void onError(AsyncEvent asyncEvent) throws IOException {
    }

    @Override
    public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
    }

    public void set(JsonPFilter.JsonPResponseWrapper responseWrapper, HttpServletResponse httpResponse,
                    String callback) {

        this.responseWrapper = responseWrapper;
        this.origResponse = httpResponse;
        this.callback = callback;
    }
}
