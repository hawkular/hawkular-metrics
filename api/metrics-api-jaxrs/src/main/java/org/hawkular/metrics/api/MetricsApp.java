/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.api;

import java.util.concurrent.ExecutorService;

import org.hawkular.commons.log.MsgLogger;
import org.hawkular.commons.log.MsgLogging;
import org.hawkular.commons.properties.HawkularProperties;
import org.hawkular.handlers.BaseApplication;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle;
import org.hawkular.metrics.api.jaxrs.MetricsServiceLifecycle.State;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class MetricsApp implements BaseApplication {
    private static final MsgLogger log = MsgLogging.getMsgLogger(MetricsApp.class);
    private static final String BASE_URL = "hawkular-merics.base-url";
    private static final String BASE_URL_DEFAULT = "/hawkular/metrics";

    ExecutorService executor;
    String baseUrl = HawkularProperties.getProperty(BASE_URL, BASE_URL_DEFAULT);

    @Override
    public void start() {
        log.infof("Metrics app started on [ %s ] ", baseUrl());

        initializeMetricsService();
    }

    public static MetricsServiceLifecycle msl;

    private static void initializeMetricsService() {
        Injector test2 = Guice.createInjector(new ServiceModule());

        msl = test2.getInstance(MetricsServiceLifecycle.class);

        msl.init();
        while (msl.getState() != State.STARTED) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(msl.getState());
        }
        System.out.println(msl.getCassandraStatus());

        System.out.println(msl.objectMapper);
    }

    @Override
    public void stop() {
        log.infof("Metrics app stopped", baseUrl());
    }

    @Override
    public String baseUrl() {
        return baseUrl;
    }
}
