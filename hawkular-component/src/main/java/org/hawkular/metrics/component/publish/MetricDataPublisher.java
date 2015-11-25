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

package org.hawkular.metrics.component.publish;

import static java.util.stream.Collectors.toList;

import static org.hawkular.bus.common.Endpoint.Type.TOPIC;

import java.io.IOException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.jms.JMSException;
import javax.jms.TopicConnectionFactory;

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.producer.ProducerConnectionContext;
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.jboss.logging.Logger;

/**
 * Publishes {@link MetricDataMessage} messages on the Hawkular bus.
 *
 * @author Thomas Segismont
 */
@ApplicationScoped
@Eager
public class MetricDataPublisher {
    private static final Logger log = Logger.getLogger(MetricDataPublisher.class);

    static final String HAWULAR_METRIC_DATA_TOPIC = "HawkularMetricData";

    @Resource(mappedName = "java:/HawkularBusConnectionFactory")
    TopicConnectionFactory topicConnectionFactory;

    private MessageProcessor messageProcessor;
    private ConnectionContextFactory connectionContextFactory;
    private ProducerConnectionContext producerConnectionContext;

    @PostConstruct
    void init() {
        messageProcessor = new MessageProcessor();
        try {
            connectionContextFactory = new ConnectionContextFactory(topicConnectionFactory);
            Endpoint endpoint = new Endpoint(TOPIC, HAWULAR_METRIC_DATA_TOPIC);
            producerConnectionContext = connectionContextFactory.createProducerConnectionContext(endpoint);
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    public void publish(Metric<? extends Number> metric) {
        BasicMessage basicMessage = createNumericMessage(metric);
        try {
            messageProcessor.send(producerConnectionContext, basicMessage);
            log.tracef("Sent message: %s", basicMessage);
        } catch (JMSException e) {
            log.warnf(e, "Could not send metric: %s", metric);
        }
    }

    private BasicMessage createNumericMessage(Metric<? extends Number> numeric) {
        MetricId<? extends Number> numericId = numeric.getMetricId();
        List<MetricDataMessage.SingleMetric> numericList = numeric.getDataPoints().stream()
                .map(dataPoint -> new MetricDataMessage.SingleMetric(numericId.getName(), dataPoint.getTimestamp(),
                        dataPoint.getValue().doubleValue()))
                .collect(toList());
        MetricDataMessage.MetricData metricData = new MetricDataMessage.MetricData();
        metricData.setTenantId(numericId.getTenantId());
        metricData.setData(numericList);
        return new MetricDataMessage(metricData);

    }

    @PreDestroy
    void shutdown() {
        if (producerConnectionContext != null) {
            try {
                producerConnectionContext.close();
            } catch (IOException ignored) {
            }
        }
        if (connectionContextFactory != null) {
            try {
                connectionContextFactory.close();
            } catch (JMSException ignored) {
            }
        }
    }
}
