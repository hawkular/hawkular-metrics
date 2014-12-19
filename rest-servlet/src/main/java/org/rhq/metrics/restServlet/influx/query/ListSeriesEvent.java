package org.rhq.metrics.restServlet.influx.query;

import javax.ws.rs.container.AsyncResponse;

/**
 * @author Thomas Segismont
 */
public class ListSeriesEvent {
    private final AsyncResponse asyncResponse;
    private final String tenantId;

    public ListSeriesEvent(AsyncResponse asyncResponse, String tenantId) {
        this.asyncResponse = asyncResponse;
        this.tenantId = tenantId;
    }

    public AsyncResponse getAsyncResponse() {
        return asyncResponse;
    }

    public String getTenantId() {
        return tenantId;
    }
}
