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

import javax.jms.JMSException;
import javax.jms.TopicConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.hawkular.bus.common.ConnectionContextFactory;
import org.hawkular.bus.common.Endpoint;
import org.hawkular.bus.common.MessageProcessor;
import org.hawkular.bus.common.ObjectMessage;
import org.hawkular.bus.common.producer.ProducerConnectionContext;

public class BusMessageSender {

    public static final String CONN_FACTORY = "/HawkularBusConnectionFactory";

    public static final String GAUGE_METRICS_TOPIC = "GaugeMetrics";
    public static final String AVAILABILITY_METRICS_TOPIC = "AvailabilityMetrics";
    public static final String COUNTER_METRICS_TOPIC = "CounterMetrics";

    public static final String GAUGE_METRICS_DATA_TOPIC = "GaugeMetricsData";
    public static final String AVAILABILITY_METRICS_DATA_TOPIC = "AvailabilityMetricsData";
    public static final String COUNTER_METRICS_DATA_TOPIC = "CounterMetricsData";

    private final ConnectionContextFactory ccf;

    public BusMessageSender() throws NamingException, JMSException {
        InitialContext ctx = new InitialContext();
        TopicConnectionFactory qconFactory = (TopicConnectionFactory) ctx.lookup(CONN_FACTORY);

        ccf = new ConnectionContextFactory(qconFactory);
    }

    public BusMessageSender(ConnectionContextFactory ccf) {
        this.ccf = ccf;
    }

    public <T> void sendMessage(T message, String destination) {
        try {
            ProducerConnectionContext pcc = ccf
                    .createProducerConnectionContext(new Endpoint(Endpoint.Type.TOPIC, destination));
            ObjectMessage msg = new ObjectMessage(message);
            new MessageProcessor().send(pcc, msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
