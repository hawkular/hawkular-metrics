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

package org.hawkular.metrics.component.insert;

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.Collections;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSRuntimeException;
import javax.jms.Queue;

import org.hawkular.bus.common.consumer.BasicMessageListener;
import org.hawkular.metrics.api.jaxrs.ServiceReady;
import org.hawkular.metrics.api.jaxrs.ServiceReadyEvent;
import org.hawkular.metrics.component.publish.AvailDataMessage;
import org.hawkular.metrics.component.publish.AvailDataMessage.AvailData;
import org.hawkular.metrics.component.publish.MetricDataMessage;
import org.hawkular.metrics.component.publish.MetricDataMessage.MetricData;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;

import rx.Observable;
import rx.Subscriber;

/**
 * Listens to new date coming from the bus and saves it with the MetricsService.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
public class NewDataListener {
    private static final Logger LOG = Logger.getLogger(NewDataListener.class);

    @Resource(mappedName = "java:/queue/hawkular/metrics/gauges/new")
    Queue gaugesQueue;

    @Resource(mappedName = "java:/queue/hawkular/metrics/counters/new")
    Queue countersQueue;

    @Resource(mappedName = "java:/queue/hawkular/metrics/availability/new")
    Queue availabilityQueue;

    @Resource(name = "java:/HawkularBusConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Inject
    MetricsService metricsService;

    private JMSContext context;
    private JMSConsumer gaugesConsumer;
    private JMSConsumer countersConsumer;
    private JMSConsumer availabilityConsumer;

    public void onMetricsServiceReady(@Observes @ServiceReady ServiceReadyEvent event) {
        context = connectionFactory.createContext();
        gaugesConsumer = context.createConsumer(gaugesQueue);
        gaugesConsumer.setMessageListener(new BasicMessageListener<MetricDataMessage>() {
            @Override
            protected void onBasicMessage(MetricDataMessage basicMessage) {
                onGaugeData(basicMessage.getMetricData());
            }
        });
        countersConsumer = context.createConsumer(countersQueue);
        countersConsumer.setMessageListener(new BasicMessageListener<MetricDataMessage>() {
            @Override
            protected void onBasicMessage(MetricDataMessage basicMessage) {
                onCounterData(basicMessage.getMetricData());
            }
        });
        availabilityConsumer = context.createConsumer(availabilityQueue);
        availabilityConsumer.setMessageListener(new BasicMessageListener<AvailDataMessage>() {
            @Override
            protected void onBasicMessage(AvailDataMessage basicMessage) {
                onAvailData(basicMessage.getAvailData());
            }
        });
    }

    private void onGaugeData(MetricData metricData) {
        Observable<Metric<Double>> metrics = Observable.from(metricData.getData()).map(singleMetric -> {
            MetricId<Double> id = new MetricId<>(metricData.getTenantId(), GAUGE, singleMetric.getSource());
            DataPoint<Double> dataPoint = new DataPoint<>(singleMetric.getTimestamp(), singleMetric.getValue());
            return new Metric<>(id, Collections.singletonList(dataPoint));
        });
        metricsService.addDataPoints(GAUGE, metrics).subscribe(new NewDataSubscriber());
    }

    private void onCounterData(MetricData metricData) {
        Observable<Metric<Long>> metrics = Observable.from(metricData.getData()).map(singleMetric -> {
            MetricId<Long> id = new MetricId<>(metricData.getTenantId(), COUNTER, singleMetric.getSource());
            long value = (long) singleMetric.getValue();
            DataPoint<Long> dataPoint = new DataPoint<>(singleMetric.getTimestamp(), value);
            return new Metric<>(id, Collections.singletonList(dataPoint));
        });
        metricsService.addDataPoints(COUNTER, metrics).subscribe(new NewDataSubscriber());
    }

    private void onAvailData(AvailData availData) {
        Observable<Metric<AvailabilityType>> metrics = Observable.from(availData.getData()).map(singleMetric -> {
            MetricId<AvailabilityType> id;
            id = new MetricId<>(singleMetric.getTenantId(), AVAILABILITY, singleMetric.getId());
            AvailabilityType availabilityType = AvailabilityType.fromString(singleMetric.getAvail());
            DataPoint<AvailabilityType> dataPoint = new DataPoint<>(singleMetric.getTimestamp(), availabilityType);
            return new Metric<>(id, Collections.singletonList(dataPoint));
        });
        metricsService.addDataPoints(AVAILABILITY, metrics).subscribe(new NewDataSubscriber());
    }

    @PreDestroy
    void shutdown() {
        closeQuietly(gaugesConsumer);
        closeQuietly(countersConsumer);
        closeQuietly(availabilityConsumer);
        if (context != null) {
            try {
                context.close();
            } catch (JMSRuntimeException ignored) {
            }
        }
    }

    private void closeQuietly(JMSConsumer jmsConsumer) {
        if (jmsConsumer != null) {
            try {
                jmsConsumer.close();
            } catch (JMSRuntimeException ignored) {
            }
        }
    }

    private static class NewDataSubscriber extends Subscriber<Void> {

        @Override
        public void onCompleted() {
        }

        @Override
        public void onError(Throwable t) {
            LOG.warn("Failed to persist data", t);
        }

        @Override
        public void onNext(Void aVoid) {
        }
    }
}
