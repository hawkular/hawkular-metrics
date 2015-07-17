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
package org.hawkular.metrics.core.bus;

import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.Endpoint.Type;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.ObjectMessage;
import org.hawkular.bus.common.consumer.ConsumerConnectionContext;
import org.hawkular.bus.common.test.SimpleTestListener;
import org.hawkular.bus.common.test.VMEmbeddedBrokerWrapper;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.MetricsService;
import org.junit.Test;
import org.testng.Assert;

import rx.observers.TestSubscriber;

/**
 * @author Stefan Negrea
 */
public class BusMessageSenderTest {

    @Test
    public void testQueryOrderedAsc() throws Exception {
        VMEmbeddedBrokerWrapper broker = new VMEmbeddedBrokerWrapper();
        broker.start();

        // create topic listener
        String brokerURL = broker.getBrokerURL();
        ConnectionContextFactory consumerFactory = new ConnectionContextFactory(brokerURL);
        Endpoint endpoint = new Endpoint(Type.TOPIC, BusMessageSender.GAUGE_METRICS_TOPIC);
        ConsumerConnectionContext consumerContext = consumerFactory.createConsumerConnectionContext(endpoint);
        SimpleTestListener<ObjectMessage> listener = new SimpleTestListener<ObjectMessage>(ObjectMessage.class);
        MessageProcessor serverSideProcessor = new MessageProcessor();
        serverSideProcessor.listen(consumerContext, listener);


        // create bus message creator service
        MetricsService serviceUnderTest = new MetricsServiceBusDelivery(consumerFactory);

        // push a message and listen for it
        TestSubscriber<Void> readSubscriber = new TestSubscriber<>();
        Metric<Double> sampleMetric = new Metric<>("123", MetricType.GAUGE, new MetricId("test"));
        serviceUnderTest.createMetric(sampleMetric).subscribe(readSubscriber);
        readSubscriber.awaitTerminalEvent();
        readSubscriber.assertNoErrors();

        listener.waitForMessage(3);

        // verify results
        ObjectMessage receivedMessage = listener.getReceivedMessage();
        Assert.assertNotNull(receivedMessage, "Should have received the message.");

        receivedMessage.setObjectClass(Metric.class);
        Metric<Double> receivedMetric = (Metric<Double>) receivedMessage.getObject();
        Assert.assertNotNull(receivedMetric, "Should have received the message with a metric payload.");

        broker.stop();
    }
}
