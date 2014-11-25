package org.rhq.metrics.restServlet;

import java.util.Map;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;

/**
 * @author John Sanda
 */
public class DataInsertedCallback implements FutureCallback<Void> {

    private AsyncResponse response;

    private String errorMsg;

    public DataInsertedCallback(AsyncResponse response, String errorMsg) {
        this.response = response;
        this.errorMsg = errorMsg;
    }

    @Override
    public void onSuccess(Void result) {
        response.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
    }

    @Override
    public void onFailure(Throwable t) {
        Map<String, String> errors = ImmutableMap.of("errorMsg", errorMsg + ": " +
            Throwables.getRootCause(t).getMessage());
        response.resume(Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(errors).type(
            MediaType.APPLICATION_JSON_TYPE).build());
    }
}
