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
package org.hawkular.metrics.component.publish;

import static org.hawkular.metrics.component.publish.PublishCommandMessage.PUBLISH_COMMAND;
import static org.hawkular.metrics.component.publish.PublishCommandMessage.UNPUBLISH_COMMAND;

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
import org.hawkular.metrics.api.jaxrs.util.Eager;
import org.jboss.logging.Logger;

/**
 * Listens to new commands to publish specific metrics on demand.
 *
 * @author Lucas Ponce
 */
@ApplicationScoped
@Eager
public class PublishCommandListener {

    private static final Logger log = Logger.getLogger(PublishCommandListener.class);

    @Resource(mappedName = "java:/queue/hawkular/metrics/publish")
    Queue publishCommandsQueue;

    /**
     * HawkularBusConnectionFactory is a non-pooled connection factory. It is important that we use a non-pooled factory
     * because JMS message listeners cannot be used in a web or enterprise application with a pooled connection factory.
     * This is mandated by the JEE spec.
     */
    @Resource(name = "java:/HawkularBusConnectionFactory")
    private ConnectionFactory connectionFactory;

    private JMSContext context;
    private JMSConsumer publishCommandsConsumer;

    @Inject
    private PublishCommandTable publishCommandTable;

    public void onMetricsServiceReady(@Observes @ServiceReady ServiceReadyEvent event) {
        context = connectionFactory.createContext();
        publishCommandsConsumer = context.createConsumer(publishCommandsQueue);
        publishCommandsConsumer.setMessageListener(new BasicMessageListener<PublishCommandMessage>() {
            @Override
            protected void onBasicMessage(PublishCommandMessage message) {
                if (null == message.getCommand() ||
                        null == message.getTenantId() ||
                        null == message.getIds()) {
                    log.warn("Received a null message");
                    return;
                }
                if (message.getCommand().equals(PUBLISH_COMMAND)) {
                    log.debugf("Publish: %s", message);
                    publishCommandTable.add(message.getTenantId(), message.getIds());
                } else if  (message.getCommand().equals(UNPUBLISH_COMMAND)) {
                    log.debugf("Unpublishing: %s", message);
                    publishCommandTable.remove(message.getTenantId(), message.getIds());
                } else {
                    log.warnf("Unrecognized command in message %s", message.toString());
                }
            }
        });
    }

    @PreDestroy
    void shutdown() {
        closeQuietly(publishCommandsConsumer);
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
}
