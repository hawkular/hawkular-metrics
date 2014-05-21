package org.rhq.metrics.rest;

import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.streams.Pump;
import org.vertx.java.platform.Verticle;

/**
 * @author John Sanda
 */
public class MetricsServer extends Verticle {

	/**
	 * CORS Handler
	 */
	public class CORSHandler extends RouteMatcher {

		public CORSHandler(final RouteMatcher routeMatcher) {
			this.all(".*", new Handler<HttpServerRequest>() {
				@Override
				public void handle(HttpServerRequest request) {
					setCORS(request);

					if (request.method().equals("OPTIONS")) {
						request.response().end();
					} else {
						routeMatcher.handle(request);
					}
				}
			});
		}

		private void setCORS(HttpServerRequest req) {
			String origin = req.headers().get("origin");
			if (origin == null || "null".equals(origin)) {
				origin = "*";
			}
			req.response().headers().set("Access-Control-Allow-Origin", origin);
			req.response().headers().set("Access-Control-Allow-Credentials", "true");
			String hdr = req.headers().get("Access-Control-Request-Headers");
			if (hdr != null) {
				req.response().headers().set("Access-Control-Allow-Headers", hdr);
			}
		}
	}

    /**
     * JSON Content-Type Handler
     */
    public class JSONHandler extends RouteMatcher {
        public JSONHandler(final RouteMatcher routeMatcher) {
            this.all(".*", new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest request) {
                    request.response().putHeader("Content-Type",
                            "application/json");

                    routeMatcher.handle(request);
                }
            });
        }
    }

    public static final String HTTP_PORT = "httpPort";

    @Override
    public void start() {
        RouteMatcher routeMatcher = new RouteMatcher();

        routeMatcher.all("/rhq-metrics/ping", new Handler<HttpServerRequest>() {
			@Override
			public void handle(HttpServerRequest request) {
				JsonObject responseBody = new JsonObject()
                .putValue("pong", new Date().toString());

				request.response().end(responseBody.toString());
			}
        });

        routeMatcher.get("/rhq-metrics/:id/data", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest request) {
                final String id = request.params().get("id");

                long start;
                String startParam = request.params().get("start");
                if (startParam == null) {
                    start = DateTime.now().minusHours(8).getMillis();
                } else {
                    start = Long.parseLong(startParam);
                }

                long end;
                String endParam = request.params().get("end");
                if (endParam == null) {
                    end = DateTime.now().getMillis();
                } else {
                    end = Long.parseLong(endParam);
                }

                JsonObject params = new JsonObject()
                    .putString("id", id)
                    .putNumber("start", start)
                    .putNumber("end", end);

                vertx.eventBus().send("rhq.metrics.get", params, new Handler<Message<JsonObject>>() {
                    @Override
                    public void handle(Message<JsonObject> message) {
                        request.response().end(message.body().toString());
                    }
                });
            }
        });

        routeMatcher.post("/rhq-metrics/:id/data", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                request.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        String id = request.params().get("id");
                        JsonObject params = new JsonObject()
                            .putString("id", id)
                            .putBinary("bytes", body.getBytes());

                        vertx.eventBus().send("rhq.metrics.addForId", params, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> message) {
                                if (message.body().containsField("error")) {
                                    request.response().write(message.body().getString("error"));
                                    request.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                                } else {
                                    request.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code());
                                }
                                request.response().end();
                            }
                        });
                    }
                });
            }
        });

        routeMatcher.get("/rhq-metrics/data", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                final List<String> ids = request.params().getAll("id");
                if (ids.isEmpty()) {
                    request.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code());
                    request.response().end();
                } else {
                    long start;
                    String startParam = request.params().get("start");
                    if (startParam == null) {
                        start = DateTime.now().minusHours(8).getMillis();
                    } else {
                        start = Long.parseLong(startParam);
                    }

                    long end;
                    String endParam = request.params().get("end");
                    if (endParam == null) {
                        end = DateTime.now().getMillis();
                    } else {
                        end = Long.parseLong(endParam);
                    }
                    final AtomicInteger count = new AtomicInteger();
                    final Pump pump = Pump.createPump(request, request.response()).start();

                    request.response().setChunked(true);

                    for (final String id : ids) {
                        JsonObject message = new JsonObject()
                            .putString("id", id)
                            .putNumber("start", start)
                            .putNumber("end", end);
                        vertx.eventBus().send("rhq.metrics.get", message, new Handler<Message<JsonObject>>() {
                            public void handle(Message<JsonObject> event) {
                                JsonObject result = event.body();
                                request.response().write(result.toString());
                                if (count.addAndGet(1) == ids.size()) {
                                    pump.stop();
                                    request.response().setStatusCode(HttpResponseStatus.OK.code());
                                    request.response().end();
                                }
                            }
                        });
                    }
                }
            }
        });

        routeMatcher.post("/rhq-metrics/data", new Handler<HttpServerRequest>() {
            public void handle(final HttpServerRequest request) {
                request.bodyHandler(new Handler<Buffer>() {
                    public void handle(Buffer body) {
                        JsonObject params = new JsonObject().putBinary("bytes", body.getBytes());
                        vertx.eventBus().send("rhq.metrics.addForIds", params, new Handler<Message<JsonObject>>() {
                            @Override
                            public void handle(Message<JsonObject> message) {
                                if (message.body().containsField("error")) {
                                    request.response().write(message.body().getString("error"));
                                    request.response().setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                                } else {
                                    request.response().setStatusCode(HttpResponseStatus.NO_CONTENT.code());
                                }
                                request.response().end();
                            }
                        });
                    }
                });
            }
        });

        vertx.createHttpServer()
                .requestHandler(new CORSHandler(new JSONHandler(routeMatcher)))
                .listen(container.config().getNumber(HTTP_PORT,
            8080).intValue());
    }

}
