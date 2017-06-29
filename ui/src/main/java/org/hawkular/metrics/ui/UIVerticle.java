package org.hawkular.metrics.ui;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

public class UIVerticle extends AbstractVerticle {

    private UIVerticle() {
    }

    public static void main(String[] args) throws InterruptedException {
        Vertx.vertx().deployVerticle(new UIVerticle());
    }

    @Override
    public void start() throws Exception {
        Router router = Router.router(vertx);

        // Create a router endpoint for the static content.
        router.route().handler(StaticHandler.create());

        // Start the web server and tell it to use the router to handle requests.
        vertx.createHttpServer().requestHandler(router::accept).listen(8081);
    }
}
