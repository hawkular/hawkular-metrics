/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.alerting;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.AvailabilityType.ADMIN;
import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.hawkular.alerts.api.model.data.Data;
import org.hawkular.alerts.api.services.AlertsServiceMock;
import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.api.jaxrs.ServiceReadyEvent;
import org.hawkular.metrics.api.jaxrs.config.ConfigurableProducer;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.Resolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import rx.Observable;

/**
 * Note that this test no longer tests filtering, as data is now filtered in alerting, not metrics.
 *
 * @author jsanda
 */
@RunWith(Arquillian.class)
public class PublishDataPointsTest {

    @Deployment
    public static WebArchive createDeployment() {
        MavenResolverSystem mavenResolver = Resolvers.use(MavenResolverSystem.class);
        List<String> deps = asList(
                "org.hawkular.metrics:hawkular-metrics-model:" + System.getProperty("version.org.hawkular.metrics"),
                "org.hawkular.metrics:hawkular-metrics-api-util:" + System.getProperty("version.org.hawkular.metrics"),
                "org.hawkular.metrics:hawkular-metrics-alerting:" + System.getProperty("version.org.hawkular.metrics"),
                "org.hawkular.alerts:hawkular-alerts-api:" + System.getProperty("version.org.hawkular.alerts"),
                "io.reactivex:rxjava:" + System.getProperty("version.io.reactivex.rxjava"));

        Collection<JavaArchive> dependencies = new HashSet<JavaArchive>();
        dependencies.addAll(asList(mavenResolver.loadPomFromFile("pom.xml").resolve(deps)
                .withoutTransitivity().as(JavaArchive.class)));

        WebArchive archive = ShrinkWrap.create(WebArchive.class)
                .addPackages(true,
                        "org.hawkular.alerts.api.services")
                .addAsLibraries(dependencies)
                .addClass(ConfigurableProducer.class)
                .addAsWebInfResource(new File("src/test/resources/web.xml"))
                .addAsWebInfResource(new File("src/test/resources/jboss-deployment-structure.xml"))
                .setManifest(new StringAsset("Manifest-Version: 1.0\nDependencies: com.google.guava\n"));

        return archive;
    }

    @Inject
    @ServiceReady
    Event<ServiceReadyEvent> serviceReadyEvent;

    @BeforeClass
    public static void setupPublishing() {
        System.setProperty("hawkular.metrics.publish-period", "100");
    }

    @Before
    public void beforeTest() {
        AlertsServiceMock.DATA.clear();
    }

    @Test
    public void publishGaugeDataPoints() throws Exception {
        String tenantId = "gauge-tenant";
        String gauge1Id = "G1";
        MetricId<Double> publishedMetricId = new MetricId<>(tenantId, GAUGE, gauge1Id);
        Metric<Double> gauge1 = new Metric<>(publishedMetricId, asList(
                new DataPoint<>(System.currentTimeMillis(), 10.0),
                new DataPoint<>(System.currentTimeMillis() - 1000, 9.0),
                new DataPoint<>(System.currentTimeMillis() - 2000, 9.0)));

        Observable<Metric<?>> observable = Observable.just(gauge1);

        serviceReadyEvent.fire(new ServiceReadyEvent(observable));

        Set<Data> expected = gauge1.getDataPoints().stream()
                .map(dataPoint -> Data.forNumeric(
                        tenantId,
                        InsertedDataSubscriber.prefixMap.get(MetricType.GAUGE) + gauge1Id,
                        dataPoint.getTimestamp(),
                        dataPoint.getValue().doubleValue()))
                .collect(Collectors.toCollection(HashSet::new));

        Thread.sleep(500);
        assertEquals(expected, AlertsServiceMock.DATA);
    }

    @Test
    public void publishAvailabilityDataPoints() throws Exception {
        String tenantId = "availability-tenant";
        String availability1Id = "A1";
        MetricId<AvailabilityType> publishedMetricId = new MetricId<>(tenantId, AVAILABILITY, availability1Id);
        Metric<AvailabilityType> availability1 = new Metric<>(publishedMetricId, asList(
                new DataPoint<>(System.currentTimeMillis(), UP),
                new DataPoint<>(System.currentTimeMillis() - 1000, DOWN),
                new DataPoint<>(System.currentTimeMillis() - 2000, UNKNOWN),
                new DataPoint<>(System.currentTimeMillis() - 3000, ADMIN)));

        Observable<Metric<?>> observable = Observable.just(availability1);

        serviceReadyEvent.fire(new ServiceReadyEvent(observable));

        Set<Data> expected = availability1.getDataPoints().stream()
                .map(dataPoint -> Data.forAvailability(
                        tenantId,
                        InsertedDataSubscriber.prefixMap.get(MetricType.AVAILABILITY) + availability1Id,
                        dataPoint.getTimestamp(),
                        toAlertingAvail(dataPoint.getValue())))
                .collect(Collectors.toCollection(HashSet::new));

        Thread.sleep(500);
        assertEquals(expected, AlertsServiceMock.DATA);
    }

    private org.hawkular.alerts.api.model.data.AvailabilityType toAlertingAvail(
            AvailabilityType metricAvailType) {
        switch (metricAvailType) {
            case UP:
                return org.hawkular.alerts.api.model.data.AvailabilityType.UP;
            case DOWN:
                return org.hawkular.alerts.api.model.data.AvailabilityType.DOWN;
            default:
                return org.hawkular.alerts.api.model.data.AvailabilityType.UNAVAILABLE;
        }
    }

}
