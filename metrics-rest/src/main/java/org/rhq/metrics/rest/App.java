package org.rhq.metrics.rest;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.platform.Verticle;

/**
 * @author John Sanda
 */
public class App extends Verticle {

    @Override
    public void start(final Future<Void> startedResult) {
        container.deployModule("org.rhq.metrics~rhq-metrics-server~0.1.0-SNAPSHOT", container.config(),
            new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                    container.deployVerticle("org.rhq.metrics.rest.MetricsServer", container.config(),
                        new Handler<AsyncResult<String>>() {
                        @Override
                        public void handle(AsyncResult<String> result1) {
                            if (result1.succeeded()) {
                                startedResult.setResult(null);
                            } else {
                                System.out.println("verticle deployment failed");
                                startedResult.setFailure(result1.cause());
                            }
                        }
                    });
                } else {
                    System.out.println("Deployment failed: " + result.cause());
                    startedResult.setFailure(result.cause());
                }
            }
        });
    }
}
