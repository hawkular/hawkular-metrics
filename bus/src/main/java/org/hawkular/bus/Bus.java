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
package org.hawkular.bus;

import java.util.Collections;
import java.util.Map;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import javax.jms.Destination;
import javax.jms.JMSConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.hawkular.bus.common.BasicMessage;
import org.hawkular.bus.common.MessageId;

/**
 * @author jsanda
 */
@Dependent
public class Bus {

    public static final String HEADER_BASIC_MESSAGE_CLASS = "basicMessageClassName";

    /**
     * HawkularBusConnectionFactory is a non-pooled connection factory. It is important that we use a non-pooled factory
     * because JMS message listeners cannot be used in a web or enterprise application with a pooled connection factory.
     * This is mandated by the JEE spec.
     */
    @Inject
    @JMSConnectionFactory("java:/HawkularBusConnectionFactory")
    private JMSContext context;

    public <T extends BasicMessage> MessageId send(Destination destination, T message) throws JMSException {
        return send(destination, message, Collections.emptyMap());
    }

    public <T extends BasicMessage> MessageId send(Destination destination, T message, Map<String, String> headers)
            throws JMSException {
        Message jmsMessage = prepareJMSMessage(message, headers);

        context.createProducer().send(destination, jmsMessage);

        message.setMessageId(new MessageId(jmsMessage.getJMSMessageID()));

        return message.getMessageId();
    }

    private <T extends BasicMessage> Message prepareJMSMessage(T message, Map<String, String> headers)
            throws JMSException {
        String json = message.toJSON();
        Message jmsMessage = context.createTextMessage(json);

        setHeaders(jmsMessage, message, headers);
        if (message.getCorrelationId() != null) {
            jmsMessage.setJMSCorrelationID(message.getCorrelationId().getId());
        }
        return jmsMessage;
    }

    private <T extends BasicMessage> void setHeaders(Message message, T basicMessage, Map<String, String> headers)
            throws JMSException {
        message.setStringProperty(HEADER_BASIC_MESSAGE_CLASS, basicMessage.getClass().getName());
        basicMessage.getHeaders().entrySet().stream().forEach(entry -> {
            try {
                message.setStringProperty(entry.getKey(), entry.getValue());
            } catch (JMSException e) {
                throw new RuntimeException("Failed to set header {key: " + entry.getKey() + ", value: " +
                        entry.getValue() + "}", e);
            }
        });
        headers.entrySet().stream().forEach(entry -> {
            try {
                message.setStringProperty(entry.getKey(), entry.getValue());
            } catch (JMSException e) {
                throw new RuntimeException("Failed to set header {key: " + entry.getKey() + ", value: " +
                        entry.getValue() + "}", e);
            }
        });
    }

}
