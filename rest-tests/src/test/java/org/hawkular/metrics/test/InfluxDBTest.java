/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.junit.BeforeClass;
/**
 * @author Jeeva Kandasamy
 */
public class InfluxDBTest {
    private static final String TENANT_PREFIX = UUID.randomUUID().toString();
    private static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0);

    static String baseURI = System.getProperty("hawkular-metrics.base-uri", "127.0.0.1:8080/hawkular/metrics");
    static InfluxDB influxDB;

    @BeforeClass
    public static void initClient() {
            influxDB = InfluxDBFactory.connect("http://"+baseURI+"/", "hawkular", "hawkular");
    }

    static String nextTenantId() {
        return "T" + TENANT_PREFIX + TENANT_ID_COUNTER.incrementAndGet();
    }
}
