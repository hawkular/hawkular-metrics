package org.rhq.metrics.restServlet;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletResponse;

/**
 * Listener that is called when the async result is available for the filter.
 * See {@link JsonPFilter} for where this is instantiated.
 * @author Heiko W. Rupp
 */
public class JsonPAsyncListener implements AsyncListener {

    private HttpServletResponse origResponse;
    private String callback;

    public JsonPAsyncListener() {
    }


    @Override
    public void onComplete(AsyncEvent asyncEvent) throws IOException {
        // Async processing is complete. We can now wrap the
        // returned output stream and set the content type.

        JsonPFilter.JsonPResponseWrapper responseWrapper = (JsonPFilter.JsonPResponseWrapper) asyncEvent
                .getSuppliedResponse();

        OutputStream responseOutputStream = origResponse.getOutputStream();

        boolean gzipped = responseWrapper.getHeader("Content-Encoding").equalsIgnoreCase("gzip");

        if (gzipped) {
            responseOutputStream = new GZIPOutputStream(responseOutputStream);
        }

        origResponse.setContentType("application/javascript; charset=utf-8");

        responseOutputStream.write((callback + "(").getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream jsonpOutputStream = responseWrapper.getByteArrayOutputStream();

        if (gzipped) {
            try (GZIPInputStream gzipInputStream = new GZIPInputStream(
                new ByteArrayInputStream(jsonpOutputStream.toByteArray()))) {
                InputStreamReader reader = new InputStreamReader(gzipInputStream,StandardCharsets.UTF_8);
                BufferedReader in = new BufferedReader(reader);

                String read;
                while ((read = in.readLine()) != null) {
                    responseOutputStream.write(read.getBytes(StandardCharsets.UTF_8));
                }
            }
        } else {
            jsonpOutputStream.writeTo(responseOutputStream);
        }

        responseOutputStream.write(");".getBytes(StandardCharsets.UTF_8));
        responseOutputStream.flush();
        if (gzipped) {
            ((GZIPOutputStream) responseOutputStream).finish();
        }
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

    public void set(HttpServletResponse httpResponse,
                    String callback) {
        this.origResponse = httpResponse;
        this.callback = callback;
    }
}
