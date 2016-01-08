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

import java.util.Collections;
import java.util.Map;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.context.Dependent;
import javax.jms.CompletionListener;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.hawkular.bus.common.BasicMessage;
import org.jboss.logging.Logger;

import rx.Observable;

/**
 * @author jsanda
 */
@Dependent
public class Bus {

    public static final String HEADER_BASIC_MESSAGE_CLASS = "basicMessageClassName";

    private static final Logger log = Logger.getLogger(Bus.class);

    /**
     * HawkularBusConnectionFactory is a non-pooled connection factory. It is important that we use a non-pooled factory
     * because JMS message listeners cannot be used in a web or enterprise application with a pooled connection factory.
     * This is mandated by the JEE spec.
     */
    @Resource(name = "java:/HawkularBusConnectionFactory")
    private ConnectionFactory connectionFactory;

    @Resource
    private ManagedExecutorService executorService;

    /**
     * Sends a message asynchronously.
     *
     * @param destination The queue or topic to which the message is being sent
     * @param message The message to send
     * @param <T> The message type which must be a subtype of {@link BasicMessage}
     * @return An {@link Observable} that emits the actual JMS message that is sent.
     */
    public <T extends BasicMessage> Observable<TextMessage> send(Destination destination, T message) {
        return Observable.create(subscriber -> {
            try {
                // We create a context per request (i.e., method invocation) in order to avoid any concurrency
                // issues since the underlying JMS session is intended for single threaded usage.
                JMSContext context = connectionFactory.createContext();
                try {
                    JMSProducer producer = context.createProducer();
                    TextMessage jmsMessage = context.createTextMessage();
                    prepareJMSMessage(message, jmsMessage, Collections.emptyMap());
                    producer.setAsync(new CompletionListener() {
                        @Override
                        public void onCompletion(Message message) {
                            subscriber.onNext((TextMessage) message);
                            close(context);
                            // TODO Should we wait until context is closed to call onCompleted?
                            subscriber.onCompleted();
                        }

                        @Override
                        public void onException(Message message, Exception exception) {
                            close(context);
                            subscriber.onError(exception);
                        }
                    });
                    producer.send(destination, jmsMessage);
                } catch (Exception e) {
                    close(context);
                    subscriber.onError(e);

                }
            } catch (Exception e) {
                // No need to call close() here since we failed to create the context
                subscriber.onError(e);
            }
        });
    }

    private <T extends BasicMessage> Message prepareJMSMessage(T message, TextMessage jmsMessage,
            Map<String, String> headers) throws JMSException {
        String json = message.toJSON();
        jmsMessage.setText(json);
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

    private void close(AutoCloseable closeable) {
        executorService.submit(() -> {
            try {
                log.debug("Closing " + closeable);
                if (closeable != null) {
                    closeable.close();
                }
            } catch (Exception e) {
                log.warn("Failed to close " + closeable, e);
            }
        });
    }

}
