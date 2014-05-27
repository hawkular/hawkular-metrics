package org.rhq.metrics;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.vertx.java.core.Context;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

import org.rhq.metrics.core.DataAccess;
import org.rhq.metrics.core.RawNumericMetric;
import org.rhq.metrics.core.SchemaManager;
import org.rhq.metrics.impl.cassandra.MetricsServiceCassandra;

/**
 * @author John Sanda
 */
public class Server extends Verticle {

    public static final String NODE_ADDRESSES = "nodes";

    public static final String KEYSPACE = "keyspace";

    public static final String CQL_PORT = "cqlPort";

    public static final String HTTP_PORT = "httpPort";

    public static final String LOG4J_CONF_FILE = "log4jConfFile";

    @Override
    public void start() {
        try {
            final ObjectMapper mapper = new ObjectMapper();

            Cluster cluster = new Cluster.Builder()
                .addContactPoints(getContactPoints())
                .withPort(container.config().getNumber(CQL_PORT, 9042).intValue())
                .build();

            updateSchemaIfNecessary(cluster);

            Session session = cluster.connect(container.config().getString(KEYSPACE, "rhq"));

            final DataAccess dataAccess = new DataAccess(session);

            final MetricsServiceCassandra metricsService = new MetricsServiceCassandra();
            metricsService.startUp(session);

            vertx.eventBus().registerHandler("rhq.metrics.get", new Handler<Message<JsonObject>>() {
                @Override
                public void handle(final Message<JsonObject> message) {
                    JsonObject request = message.body();
                    final String id = request.getString("id");
                    long start = request.getNumber("start").longValue();
                    long end = request.getNumber("end").longValue();

                    final Context context = vertx.currentContext();

                    ListenableFuture<List<RawNumericMetric>> future = metricsService.findData("raw", id, start, end);
                    Futures.addCallback(future, new FutureCallback<List<RawNumericMetric>>() {
                        public void onSuccess(List<RawNumericMetric> metrics) {
                            final JsonObject result = new JsonObject();
                            result.putString("bucket", "raw");
                            result.putString("id", id);
                            JsonArray data = new JsonArray();

                            for (RawNumericMetric metric : metrics) {
                                JsonObject jsonRow = new JsonObject();
                                jsonRow.putNumber("time", metric.getTimestamp());
                                jsonRow.putNumber("value", metric.getValue());
                                data.addObject(jsonRow);
                            }
                            result.putArray("data", data);

                            context.runOnContext(new Handler<Void>() {
                                @Override
                                public void handle(Void aVoid) {
                                    message.reply(result);
                                }
                            });
                        }

                        public void onFailure(final Throwable t) {
                            context.runOnContext(new Handler<Void>() {
                                @Override
                                public void handle(Void aVoid) {
                                    JsonObject result = new JsonObject().putString("error", t.getMessage());
                                    message.reply(result);
                                }
                            });
                        }
                    });
                }
            });

            vertx.eventBus().registerHandler("rhq.metrics.addForId", new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> message) {
                    try {
                        String id = message.body().getString("id");
                        CollectionType collectionTye = TypeFactory.defaultInstance().constructCollectionType(Set.class,
                            RawNumericMetric.class);
                        Set<RawNumericMetric> rawData = mapper.readValue(message.body().getBinary("bytes"),
                            collectionTye);

                        for (RawNumericMetric raw : rawData) {
                            raw.setId(id);
                        }
                        metricsService.addData(rawData);

                        message.reply(new JsonObject());
                    } catch (IOException e) {
                        message.reply(new JsonObject().putString("error", e.getMessage()));
                    }

                }
            });

            vertx.eventBus().registerHandler("rhq.metrics.addForIds", new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> message) {
                    try {
                        CollectionType collectionTye = TypeFactory.defaultInstance().constructCollectionType(Set.class,
                            RawNumericMetric.class);
                        Set<RawNumericMetric> rawData = mapper.readValue(message.body().getBinary("bytes"),
                            collectionTye);

                        metricsService.addData(rawData);

                        message.reply(new JsonObject());
                    } catch (IOException e) {
                        message.reply(new JsonObject().putString("error", e.getMessage()));
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String[] getContactPoints() {
        JsonArray addresses = container.config().getArray(NODE_ADDRESSES);
        if (addresses == null) {
            return new String[] {"127.0.0.1"};
        } else {
            String[] contactPoints = new String[addresses.size()];
            for (int i = 0; i < addresses.size(); ++i) {
                contactPoints[i] = addresses.get(i);
            }
            return contactPoints;
        }
    }

    private void updateSchemaIfNecessary(Cluster cluster) {
        try (Session session = cluster.connect("system")) {
            SchemaManager schemaManager = new SchemaManager(session);
            schemaManager.updateSchema(container.config().getString(KEYSPACE, "rhq"));
        }
    }

}
