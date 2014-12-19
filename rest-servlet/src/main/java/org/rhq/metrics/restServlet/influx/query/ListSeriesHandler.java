package org.rhq.metrics.restServlet.influx.query;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.rhq.metrics.core.Metric;
import org.rhq.metrics.core.MetricType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class ListSeriesHandler {

    @Inject
    private MetricsService metricsService;

    @Resource
    private ManagedExecutorService executor;

    public void listSeries(@Observes ListSeriesEvent listSeriesEvent) {
        ListenableFuture<List<Metric>> future = metricsService.findMetrics(listSeriesEvent.getTenantId(),
            MetricType.NUMERIC);
        Futures.addCallback(future, new FutureCallback<List<Metric>>() {
            @Override
            public void onSuccess(List<Metric> result) {
                List<InfluxObject> objects = new ArrayList<>(result.size());

                for (Metric metric : result) {
                    InfluxObject obj = new InfluxObject(metric.getId().getName());
                    obj.columns = new ArrayList<>(2);
                    obj.columns.add("time");
                    obj.columns.add("sequence_number");
                    obj.columns.add("val");
                    obj.points = new ArrayList<>(1);
                    objects.add(obj);
                }

                Response.ResponseBuilder builder = Response.ok(objects);

                listSeriesEvent.getAsyncResponse().resume(builder.build());
            }

            @Override
            public void onFailure(Throwable t) {
                listSeriesEvent.getAsyncResponse().resume(t);
            }
        }, executor);
    }

}
