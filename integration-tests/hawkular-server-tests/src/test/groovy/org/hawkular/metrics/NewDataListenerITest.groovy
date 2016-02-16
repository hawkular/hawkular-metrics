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
package org.hawkular.metrics

import static java.util.UUID.randomUUID
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.MINUTES
import static org.hawkular.metrics.BaseITest.host
import static org.hawkular.metrics.BaseITest.httpPort
import static org.junit.Assert.assertEquals

import java.util.concurrent.CountDownLatch

import javax.jms.ConnectionFactory
import javax.jms.JMSConsumer
import javax.jms.JMSContext
import javax.jms.JMSProducer
import javax.jms.Queue
import javax.jms.TextMessage
import javax.jms.Topic
import javax.naming.Context
import javax.naming.InitialContext

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import groovy.json.JsonBuilder
import groovy.json.JsonSlurper

/**
 * @author Thomas Segismont
 */
class NewDataListenerITest extends BaseITest {
  static String tenantId

  static Context jndiContext
  static ConnectionFactory connectionFactory

  static Queue gaugesQueue
  static Queue countersQueue
  static Queue availabilityQueue

  static Topic metricDataTopic
  static Topic availabilityDataTopic

  static JMSContext jmsProducerContext
  static JMSProducer producer
  static JMSContext jmsConsumerContext
  static JMSConsumer metricDataTopicConsumer
  static JMSConsumer availabilityDataTopicConsumer

  long now = System.currentTimeMillis()

  @BeforeClass
  static void setup() {
    tenantId = BaseITest.getTenantId()

    Properties env = new Properties()
    env.put(Context.INITIAL_CONTEXT_FACTORY, 'org.jboss.naming.remote.client.InitialContextFactory')
    env.put(Context.PROVIDER_URL, System.getProperty(Context.PROVIDER_URL, "http-remoting://${host}:${httpPort}"))

    jndiContext = new InitialContext(env)

    connectionFactory = (ConnectionFactory) jndiContext.lookup('jms/RemoteConnectionFactory')
    gaugesQueue = (Queue) jndiContext.lookup('queue/hawkular/metrics/gauges/new')
    countersQueue = (Queue) jndiContext.lookup('queue/hawkular/metrics/counters/new')
    availabilityQueue = (Queue) jndiContext.lookup('queue/hawkular/metrics/availability/new')

    metricDataTopic = (Topic) jndiContext.lookup('topic/HawkularMetricData')
    availabilityDataTopic = (Topic) jndiContext.lookup('topic/HawkularAvailData')

    def userName = 'jmsuser'
    def password = 'password'
    jmsProducerContext = connectionFactory.createContext(userName, password)
    producer = jmsProducerContext.createProducer()
    jmsConsumerContext = connectionFactory.createContext(userName, password)
    metricDataTopicConsumer = jmsConsumerContext.createConsumer(metricDataTopic)
    availabilityDataTopicConsumer = jmsConsumerContext.createConsumer(availabilityDataTopic)
  }

  @Before
  @After
  void beforeAndAfterTest() {
    metricDataTopicConsumer.setMessageListener(null);
  }

  @Test
  void testAddGaugeData() {
    testAddNumericData(gaugesQueue, 3.45, BigDecimal.class)
  }

  @Test
  void testAddCounterData() {
    testAddNumericData(countersQueue, 29, Integer.class)
  }

  void testAddNumericData(Queue inputQueue, Number initValue, Class<? extends Number> comparisonClass) {
    def expectedMetrics = []
    def inputMessages = []
    10.times { i ->
      def source = randomUUID().toString()
      expectedMetrics.push(source)
      10.times { j ->
        def timestamp = now - MILLISECONDS.convert(j + 1, MINUTES)
        inputMessages.push(metricDataMessage(source, timestamp, initValue + j))
      }
    }

    def latch = new CountDownLatch(100)
    def data = []
    metricDataTopicConsumer.setMessageListener({ message ->
      def insertedData = new JsonSlurper().parseText(((TextMessage) message).text)
      if (insertedData.metricData.tenantId.equals(tenantId)
          && insertedData.metricData.data.size() == 1
          && expectedMetrics.contains(insertedData.metricData.data[0].source)) {
        data.push(insertedData.metricData.data[0])
        latch.countDown()
      }
    })

    inputMessages.each { it ->
      def json = new JsonBuilder(it).toString()
      producer.send(inputQueue, json)
    }

    latch.await(1, MINUTES)

    assertEquals(expectedMetrics as Set, data.groupBy { it -> it.source }.keySet())

    data.groupBy { it -> it.source }.each { Map.Entry entry ->
      def expected = []
      10.times { i ->
        def point = [source: entry.key, timestamp: now - MILLISECONDS.convert(i + 1, MINUTES), value: (initValue + i).asType(comparisonClass)]
        expected.push(point)
      }
      def actual = (entry.value as List).sort({ p1, p2 -> Long.compare(p2.timestamp, p1.timestamp) }).collect { point ->
        point.value = point.value.asType(comparisonClass)
        point
      }
      assertEquals(expected, actual)
    }
  }

  private static Map metricDataMessage(String source, long timestamp, Number value) {
    [
        metricData: [
            tenantId: tenantId,
            data    : [
                [source: source, timestamp: timestamp, value: value]
            ]
        ]
    ]
  }

  @Test
  void testAddAvailabilityData() {
    def expectedMetrics = []
    def inputMessages = []
    10.times { i ->
      def id = randomUUID().toString()
      expectedMetrics.push(id)
      10.times { j ->
        def timestamp = now - MILLISECONDS.convert(j + 1, MINUTES)
        inputMessages.push(availDataMessage(id, timestamp, j % 2 == 0 ? 'up' : 'down'))
      }
    }

    def latch = new CountDownLatch(100)
    def data = []
    availabilityDataTopicConsumer.setMessageListener({ message ->
      def insertedData = new JsonSlurper().parseText(((TextMessage) message).text)
      if (insertedData.availData.data.size() == 1
          && insertedData.availData.data[0].tenantId.equals(tenantId)
          && expectedMetrics.contains(insertedData.availData.data[0].id)) {
        data.push(insertedData.availData.data[0])
        latch.countDown()
      }
    })

    inputMessages.each { it ->
      def json = new JsonBuilder(it).toString()
      producer.send(availabilityQueue, json)
    }

    latch.await(1, MINUTES)

    assertEquals(expectedMetrics as Set, data.groupBy { it -> it.id }.keySet())

    data.groupBy { it -> it.id }.each { Map.Entry entry ->
      def expected = []
      10.times { i ->
        def point = [tenantId: tenantId, id: entry.key, timestamp: now - MILLISECONDS.convert(i + 1, MINUTES), avail: i % 2 == 0 ? 'up' : 'down']
        expected.push(point)
      }
      def actual = (entry.value as List).sort({ p1, p2 -> Long.compare(p2.timestamp, p1.timestamp) }).collect({ point ->
        point.avail = (point.avail as String).toLowerCase()
        point
      })
      assertEquals(expected, actual)
    }
  }

  private static Map availDataMessage(String id, long timestamp, String value) {
    [
        availData: [
            data: [
                [tenantId: tenantId, id: id, timestamp: timestamp, avail: value]
            ]
        ]
    ]
  }

  @AfterClass
  static void tearDown() {
    try {
      jmsProducerContext?.close()
    } catch (Exception ignored) {
    }
    try {
      jmsConsumerContext?.close()
    } catch (Exception ignored) {
    }
    try {
      jndiContext?.close()
    } catch (Exception ignored) {
    }
  }

}
