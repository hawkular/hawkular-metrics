package org.rhq.metrics.rest;

import java.io.IOException;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import org.joda.time.DateTime;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import org.rhq.metrics.core.DataAccess;
import org.rhq.metrics.core.DataType;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.RawNumericMetric;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author John Sanda
 */
public class MetricsServer extends Verticle {

    @Override
    public void start() {
        Cluster cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect("rhq");

        DataAccess dataAccess = new DataAccess(session);

        final MetricsService metricsService = new MetricsService();
        metricsService.setDataAccess(dataAccess);

        final ObjectMapper mapper = new ObjectMapper();

        RouteMatcher routeMatcher = new RouteMatcher();
        routeMatcher.get("/rhq-metrics/:id", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final String id = request.params().get("id");
//                long start = Long.parseLong(request.params().get("start"));
//                long end = Long.parseLong(request.params().get("end"));
                long start = DateTime.now().minusHours(8).getMillis();
                long end = DateTime.now().getMillis();
                ResultSetFuture future = metricsService.findData("raw", id, start, end);

                Futures.addCallback(future, new FutureCallback<ResultSet>() {
                    public void onSuccess(ResultSet resultSet) {
                        JsonObject result = new JsonObject();
                        result.putString("bucket", "raw");
                        result.putString("id", id);
                        JsonArray data = new JsonArray();

                        for (Row row : resultSet) {
                            Map<Integer, Double> map = row.getMap(2, Integer.class, Double.class);
                            JsonObject jsonRow = new JsonObject();
                            jsonRow.putNumber("time", row.getDate(1).getTime());
                            jsonRow.putNumber("value", map.get(DataType.RAW.ordinal()));
                            data.addObject(jsonRow);
                        }
                        result.putArray("data", data);

                        request.response().end(result.toString());
                    }

                    public void onFailure(Throwable t) {
                        request.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                        request.response().setStatusMessage("Failed to retrieve data: " + t.getMessage());
                        request.response().end();
                    }
                });
            }
        });

        routeMatcher.post("/rhq-metrics/:id", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                request.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        try {
                            RawData rawData = mapper.readValue(body.getBytes(), RawData.class);

                            metricsService.addData(ImmutableSet.of(new RawNumericMetric(rawData.id, rawData.value,
                                rawData.timestamp)));

                            request.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code());
                            request.response().end();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        });

        vertx.createHttpServer().requestHandler(routeMatcher).listen(7474);
    }
}
