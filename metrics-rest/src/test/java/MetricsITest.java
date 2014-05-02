import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.PlatformLocator;
import org.vertx.java.platform.PlatformManager;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author John Sanda
 */
public class MetricsITest {

    private PlatformManager platformManager;

    @BeforeClass
    public void startModule() throws Exception {
        final CountDownLatch moduleDeployed = new CountDownLatch(1);
        final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

        platformManager = PlatformLocator.factory.createPlatformManager();
        String moduleName = System.getProperty("module.name");
        platformManager.deployModule(moduleName, null, 1, new Handler<AsyncResult<String>>() {
            public void handle(AsyncResult<String> result) {
                if (result.failed()) {
                    System.out.println("MODULE DEPLOYMENT FAILED");
                    cause.set(result.cause());
                    result.cause().printStackTrace();
                } else {
                    System.out.println("MODULE DEPLOYED");
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


    @Test
    public void insertSingleMetric() throws Exception {
        final CountDownLatch responseReceived = new CountDownLatch(1);
        final AtomicInteger status = new AtomicInteger();
        HttpClient httpClient = platformManager.vertx().createHttpClient();
        httpClient.setPort(7474);
        HttpClientRequest request = httpClient.post("/rhq-metrics/100", new Handler<HttpClientResponse>() {
            public void handle(HttpClientResponse response) {
                status.set(response.statusCode());
                responseReceived.countDown();
            }
        });
        String data = new JsonObject()
            .putString("id", "111")
            .putNumber("value", 3.14)
            .putNumber("timestamp", System.currentTimeMillis())
            .toString();
        request.headers().add("Content-Length", Integer.toString(data.length()));
        request.write(data);
        request.end();

        responseReceived.await();

        assertEquals(status.get(), HttpResponseStatus.NO_CONTENT.code(), "The status code is wrong");
    }

}
