import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;

import org.rhq.metrics.core.DataAccess;
import org.rhq.metrics.core.DataType;
import org.rhq.metrics.core.RawMetricMapper;
import org.rhq.metrics.core.RawNumericMetric;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author John Sanda
 */
public class MetricsITest {

    private static final long FUTURE_TIMEOUT = 3;

    private static final int TTL = 360;

    private PlatformManager platformManager;

    private Session session;

    private DataAccess dataAccess;

    private RawMetricMapper rawMapper;

    @BeforeClass
    public void startModule() throws Exception {
        Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("rhq");

        dataAccess = new DataAccess(session);

        rawMapper = new RawMetricMapper();

        final CountDownLatch moduleDeployed = new CountDownLatch(1);
        final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

        platformManager = PlatformLocator.factory.createPlatformManager();
        String moduleName = System.getProperty("module.name");
        platformManager.deployModule(moduleName, null, 1, new Handler<AsyncResult<String>>() {
            public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                    cause.set(result.cause());
                    result.cause().printStackTrace();
                }
                moduleDeployed.countDown();
            }
        });
        moduleDeployed.await();
        assertNull(cause.get(), "Deployment of " + moduleName + " failed");
    }

    @AfterClass
    public void stopModule() {
        platformManager.stop();
    }

    // Not sure if it is a client side bug or something I am doing wrong but we need to
    // test the GET request after the POST request. See
    // https://bugs.eclipse.org/bugs/show_bug.cgi?id=434064 for details.
    @Test(dependsOnMethods = "insertMultipleMetrics")
    public void findRawMetrics() throws Exception {
        session.execute("TRUNCATE metrics");

        String metricId = UUID.randomUUID().toString();
        long timestamp1 = System.currentTimeMillis();
        double value1 = 2.17;

        ResultSetFuture insertFuture = dataAccess.insertData("raw", metricId, timestamp1,
            ImmutableMap.of(DataType.RAW.ordinal(), value1), TTL);
        // wait for the insert to finish
        getUninterruptibly(insertFuture);

        long timestamp2 = timestamp1 - 2500;
        double value2 = 2.4567;

        insertFuture = dataAccess.insertData("raw", metricId, timestamp2, ImmutableMap.of(DataType.RAW.ordinal(),
            value2), TTL);
        // wait for insert to finish
        getUninterruptibly(insertFuture);

        final CountDownLatch responseReceived = new CountDownLatch(1);
        final Buffer buffer = new Buffer();

        HttpClient httpClient = platformManager.vertx().createHttpClient().setPort(7474);
        httpClient.getNow("/rhq-metrics/" + metricId + "/data", new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse response) {
                response.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer content) {
                        buffer.appendBuffer(content);
                        responseReceived.countDown();
                    }
                });
            }
        });
        responseReceived.await();

        JsonObject actual = new JsonObject(buffer.toString());

        JsonObject expected = new JsonObject()
            .putString("bucket", "raw")
            .putString("id", metricId)
            .putArray("data", new JsonArray()
                .addElement(new JsonObject()
                    .putNumber("time", timestamp2)
                    .putNumber("value", value2))
                .addElement(new JsonObject()
                    .putNumber("time", timestamp1)
                    .putNumber("value", value1)));

        assertEquals(actual, expected, "The data returned for GET /rhq-metrics/" + metricId + "/data" +
            " does not match the expected value");
    }

    @Test
    public void insertSingleMetric() throws Exception {
        session.execute("TRUNCATE metrics");

        final CountDownLatch responseReceived = new CountDownLatch(1);
        final AtomicInteger status = new AtomicInteger();

        JsonObject json = new JsonObject()
            .putString("id", "111")
            .putNumber("value", 3.14)
            .putNumber("timestamp", System.currentTimeMillis());
        int contentLength = json.toString().length();

        HttpClient httpClient = platformManager.vertx().createHttpClient().setPort(7474);
        HttpClientRequest request = httpClient.post("/rhq-metrics/111/data", new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse response) {
                status.set(response.statusCode());
                responseReceived.countDown();
            }
        });

        request.putHeader("Content-Length", Integer.toString(contentLength));
        request.write(json.toString());
        request.end();

        responseReceived.await();

        assertEquals(status.get(), HttpResponseStatus.NO_CONTENT.code(), "The status code is wrong");

        ResultSetFuture future = dataAccess.findData("raw", json.getString("id"),
            json.getNumber("timestamp").longValue(), System.currentTimeMillis());
        ResultSet resultSet = getUninterruptibly(future);

        List<RawNumericMetric> metrics = rawMapper.map(resultSet);

        assertEquals(metrics.size(), 1, "Expected to get back one row");

        RawNumericMetric actual = metrics.get(0);
        RawNumericMetric expected = new RawNumericMetric(json.getString("id"), json.getNumber("value").doubleValue(),
            json.getNumber("timestamp").longValue());

        assertEquals(actual, expected, "The raw metric does not match the expected value");
    }

    @Test
    public void insertMultipleMetrics() throws Exception {
        session.execute("TRUNCATE metrics");

        final CountDownLatch responseReceived = new CountDownLatch(1);
        final AtomicInteger status = new AtomicInteger();

        long collectionTime = System.currentTimeMillis();

        JsonArray json = new JsonArray()
            .addObject(new JsonObject()
                .putString("id", "100")
                .putNumber("timestamp", collectionTime + 100)
                .putNumber("value", 100))
            .addObject(new JsonObject()
                .putString("id", "200")
                .putNumber("timestamp", collectionTime + 200)
                .putNumber("value", 200))
            .addObject(new JsonObject()
                .putString("id", "300")
                .putNumber("timestamp", collectionTime + 300)
                .putNumber("value", 300));
        int contentLength = json.toString().length();

        HttpClient httpClient = platformManager.vertx().createHttpClient().setPort(7474);
        HttpClientRequest request = httpClient.post("/rhq-metrics/data", new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse response) {
                status.set(response.statusCode());
                responseReceived.countDown();
            }
        });

        request.putHeader("Content-Length", Integer.toString(contentLength));
        request.write(json.toString());
        request.end();

        responseReceived.await();

        assertEquals(status.get(), HttpResponseStatus.NO_CONTENT.code(), "The status code is wrong");

        ResultSet resultSet = session.execute("SELECT metric_id, time, value FROM metrics");
        List<RawNumericMetric> actual = rawMapper.map(resultSet);
        List<RawNumericMetric> expected = ImmutableList.of(
            new RawNumericMetric("100", 100.0, collectionTime + 100),
            new RawNumericMetric("200", 200.0, collectionTime + 200),
            new RawNumericMetric("300", 300.0, collectionTime + 300)
        );

        assertEquals(actual, expected, "The data returned from the database does not match the expected values");
    }

    private <V> V getUninterruptibly(Future<V> future) throws ExecutionException, TimeoutException {
        return Uninterruptibles.getUninterruptibly(future, FUTURE_TIMEOUT, TimeUnit.SECONDS);
    }

}
