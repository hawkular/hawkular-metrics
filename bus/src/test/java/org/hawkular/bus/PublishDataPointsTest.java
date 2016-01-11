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
package org.hawkular.bus;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.enterprise.event.Event;
import javax.inject.Inject;
import javax.jms.JMSConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.hawkular.bus.common.AbstractMessage;
import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.api.jaxrs.ServiceReadyEvent;
import org.hawkular.metrics.component.publish.AvailDataMessage;
import org.hawkular.metrics.component.publish.MetricDataMessage;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.Resolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenResolverSystem;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.fasterxml.jackson.databind.ObjectMapper;

import rx.Observable;

/**
 * @author jsanda
 */
@RunWith(Arquillian.class)
public class PublishDataPointsTest {

    @Deployment
    public static WebArchive createDeployment() {
        String projectVersion = "0.11.0.Final";
        MavenResolverSystem mavenResolver = Resolvers.use(MavenResolverSystem.class);
        List<String> deps = asList(
                "org.hawkular.metrics:hawkular-metrics-model:" + projectVersion,
                "org.hawkular.metrics:hawkular-metrics-api-util:" + projectVersion,
                "org.hawkular.commons:hawkular-bus-common:0.3.2.Final",
                "io.reactivex:rxjava:1.0.13"
        );

        Collection<JavaArchive> dependencies = new HashSet<JavaArchive>();
        dependencies.addAll(asList(mavenResolver.loadPomFromFile("pom.xml").resolve(deps)
                .withoutTransitivity().as(JavaArchive.class)));

        WebArchive archive = ShrinkWrap.create(WebArchive.class)
                .addPackages(true, "org.hawkular.bus", "org.hawkular.metrics.component.publish")
                .addAsLibraries(dependencies)
                .setManifest(new StringAsset("Manifest-Version: 1.0\nDependencies: com.google.guava\n"));

//        ZipExporter exporter = new ZipExporterImpl(archive);
//        exporter.exportTo(new File("target", "test-archive.war"));
        return archive;
    }

    @Inject
    Bus bus;

    @Resource(mappedName = "java:/topic/HawkularMetricData")
    Topic gaugeDataTopic;

    @Resource(mappedName = "java:/topic/HawkularAvailData")
    Topic availabilityDataTopic;

    @Inject
    @JMSConnectionFactory("java:/HawkularBusConnectionFactory")
    JMSContext context;

    @Inject
    @ServiceReady
    Event<ServiceReadyEvent> serviceReadyEvent;

    @Test
    public void publishGaugeDataPoints() throws Exception {
        String tenantId = "gauge-tenant";
        String gaugeId = "G1";
        Metric<Double> gauge = new Metric<>(new MetricId<>(tenantId, GAUGE, gaugeId), asList(
                new DataPoint<>(System.currentTimeMillis(), 10.0),
                new DataPoint<>(System.currentTimeMillis() - 1000, 9.0),
                new DataPoint<>(System.currentTimeMillis() - 2000, 9.0)
        ));
        Observable<Metric<?>> observable = Observable.just(gauge);

        MetricMessageListener<MetricDataMessage> listener = new MetricMessageListener<>(1, MetricDataMessage.class);
        context.createConsumer(gaugeDataTopic).setMessageListener(listener);

        serviceReadyEvent.fire(new ServiceReadyEvent(observable));

        MetricDataMessage.MetricData data = new MetricDataMessage.MetricData();
        data.setTenantId(tenantId);
        data.setData(gauge.getDataPoints().stream()
                .map(dataPoint -> new MetricDataMessage.SingleMetric(gaugeId, dataPoint.getTimestamp(),
                        dataPoint.getValue()))
                .collect(toList()));
        List<MetricDataMessage> expected = Collections.singletonList(new MetricDataMessage(data));
        List<MetricDataMessage> actual = listener.getMessages(5, TimeUnit.SECONDS);

        assertEquals(expected, actual);
    }

    @Test
    public void publishAvailabilityDataPoints() throws Exception {
        String tenantId = "availability-tenant";
        String metricId = "A1";
        Metric<AvailabilityType> availability = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, metricId), asList(
                new DataPoint<>(System.currentTimeMillis(), UP),
                new DataPoint<>(System.currentTimeMillis() - 1000, DOWN),
                new DataPoint<>(System.currentTimeMillis() - 2000, UNKNOWN)
        ));

        Observable<Metric<?>> observable = Observable.just(availability);

        MetricMessageListener<AvailDataMessage> listener = new MetricMessageListener<>(1, AvailDataMessage.class);
        context.createConsumer(availabilityDataTopic).setMessageListener(listener);

        serviceReadyEvent.fire(new ServiceReadyEvent(observable));

        AvailDataMessage.AvailData data = new AvailDataMessage.AvailData();
        data.setData(availability.getDataPoints().stream()
                .map(dataPoint -> new AvailDataMessage.SingleAvail(tenantId, metricId, dataPoint.getTimestamp(),
                        dataPoint.getValue().getText().toUpperCase()))
                .collect(toList()));
        List<AvailDataMessage> expected = Collections.singletonList(new AvailDataMessage(data));
        List<AvailDataMessage> actual = listener.getMessages(5, TimeUnit.SECONDS);

        assertEquals(expected, actual);
    }

    private class MetricMessageListener<T extends AbstractMessage> implements MessageListener {

        private List<TextMessage> messages;
        private CountDownLatch latch;
        private Class<T> messageClass;
        private ObjectMapper mapper;

        public MetricMessageListener(int expectedMessageCount, Class<T> messageClass) {
            messages = new CopyOnWriteArrayList<>();
            latch = new CountDownLatch(expectedMessageCount);
            mapper = new ObjectMapper();
            this.messageClass = messageClass;
        }

        @Override
        public void onMessage(Message message) {
            messages.add((TextMessage) message);
            latch.countDown();
        }

        public List<T> getMessages(long timeout, TimeUnit timeUnit) throws Exception {
            latch.await(timeout, timeUnit);
            return messages.stream().map(this::parseMessage).collect(toList());
        }

        private T parseMessage(TextMessage message) {
            try {
                return mapper.readValue(message.getText(), messageClass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
