package org.hawkular.metrics.api.servlet;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.hawkular.metrics.api.filter.TenantFilter;
import org.hawkular.metrics.api.model.GaugeDataPoint;
import org.hawkular.metrics.api.servlet.rx.ObservableServlet;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.impl.MetricsServiceImpl;
import org.hawkular.metrics.tasks.api.Task2;
import org.hawkular.metrics.tasks.api.TaskScheduler;
import org.hawkular.metrics.tasks.api.Trigger;
import org.hawkular.metrics.tasks.impl.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Created by miburman on 8/13/15.
 */
@WebServlet(urlPatterns = "/hawkular/metrics/*", asyncSupported = true)
public class MetricsServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsServlet.class);

    private static final ObjectMapper objectMapper;
    private static final ObjectReader objectReader;

    static {
        objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader(GaugeDataPoint[].class);
    }

    private MetricsServiceImpl metricsService;

    @Override
    public void init() throws ServletException {
        Session session = createSession();
        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(new FakeTaskScheduler());
        metricsService.startUp(session, "hawkular_metrics", false, false, new MetricRegistry());
        LOGGER.info("Metrics service started");
    }

    @Override
    public void destroy() {
        metricsService.shutdown();
    }

    private Session createSession() {
        Cluster.Builder clusterBuilder = new Cluster.Builder();
        clusterBuilder.withPort(9042);
        clusterBuilder.addContactPoint("127.0.0.1");

        Cluster cluster = null;
        Session createdSession = null;
        try {
            cluster = clusterBuilder.build();
            createdSession = cluster.connect("system");
            return createdSession;
        } finally {
            if (createdSession == null && cluster != null) {
                cluster.close();
            }
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Always use getRequestURI, it's not decoded. Does not have queryparams (stuff after ?)
        System.out.println("RequestURI: " + req.getRequestURI());
        // To get QueryParams, use the following
        System.out.println("QueryString: " + req.getQueryString());

        String tenantId = getTenantId(req);
        String[] queryParts = getQueryParts(req);

        MetricType type = MetricType.fromTextCode(queryParts[0]);
        MetricId id = getMetricId(tenantId, type, queryParts[1]);

        AsyncContext asyncContext = getAsyncContext(req);

        System.out.println("MetricId: " + id.toString());
        asyncContext.complete();
    }

//    public static Observable<Metric<Double>> datapointsToMetric(GaugeDataPoint[] gaugeDataPoints) {
//        Observable.from(gaugeDataPoints)
//                .map(p -> new DataPoint<>(p.getTimestamp(), p.getValue(), p.getTags()));
//    }

    public static List<DataPoint<Double>> requestToGaugeDataPoints(GaugeDataPoint[] gaugeDataPoints) {
        return Arrays.stream(gaugeDataPoints)
                .map(p -> new DataPoint<>(p.getTimestamp(), p.getValue(), p.getTags()))
                .collect(toList());
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String tenantId = getTenantId(req);
        String[] queryParts = getQueryParts(req);

        MetricType type = MetricType.fromTextCode(queryParts[0]);
        MetricId id = getMetricId(tenantId, type, queryParts[1]);

        AsyncContext asyncContext = getAsyncContext(req);

        PublishSubject<Metric<Double>> gaugeSubject = PublishSubject.create();

        getInput(req)
                .map(b -> {
                    b.flip();
                    byte[] reply = new byte[b.remaining()];
                    b.get(reply);
                    try {
                        return objectReader.readValue(reply);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(data -> new Metric<>(id, requestToGaugeDataPoints((GaugeDataPoint[]) data)))
                .subscribe(gaugeSubject::onNext,
                        gaugeSubject::onError,
                        gaugeSubject::onCompleted);

        metricsService.addGaugeData(gaugeSubject)
                .subscribe(v -> {
                        },
                        t -> {
                            t.printStackTrace();
                            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                            asyncContext.complete();
                        },
                        () -> {
                            resp.setStatus(HttpServletResponse.SC_CREATED);
                            asyncContext.complete();
                        }
                );
    }

    private AsyncContext getAsyncContext(HttpServletRequest req) {
        if(req.isAsyncStarted()) {
            return req.getAsyncContext();
        } else {
            return req.startAsync();
        }
    }

    private Observable<ByteBuffer> getInput(HttpServletRequest req) throws IOException {
        // We know each Observed ByteBuffer is going to be max. 4096 bytes
        int bufferSize = getNextPowerOf2(req.getContentLength());

        return ObservableServlet.create(req.getInputStream())
                .reduce(ByteBuffer.allocate(bufferSize), ByteBuffer::put);
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

    private String[] getQueryParts(HttpServletRequest req) {

        String servletPath = req.getServletPath();
        String requestURI = req.getRequestURI();

        String queryURI = requestURI.substring(requestURI.lastIndexOf(servletPath) + servletPath.length() + 1);
        return queryURI.split("/");
    }

    private MetricId getMetricId(String tenantId, MetricType type, String queryName) {
        try {
            String name = URLDecoder.decode(queryName, "UTF-8");
            return new MetricId(tenantId, type, name);
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    private String getTenantId(HttpServletRequest req) {
        return (String) req.getAttribute(TenantFilter.TENANT_HEADER_NAME);
    }

    private class FakeTaskScheduler implements TaskScheduler {

        @Override
        public Observable<Lease> start() {
            return Observable.empty();
        }

        @Override
        public Observable<Task2> scheduleTask(String name, String groupKey, int executionOrder,
                                              Map<String, String> parameters, Trigger trigger) {
            return Observable.empty();
        }

        @Override
        public void shutdown() {
        }

        @Override
        public Subscription subscribe(Action1<Task2> onNext) {
            return Subscribers.empty();
        }

        @Override
        public Subscription subscribe(Subscriber<Task2> subscriber) {
            return Subscribers.empty();
        }

        @Override
        public Observable<Long> getFinishedTimeSlices() {
            return Observable.empty();
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

}
