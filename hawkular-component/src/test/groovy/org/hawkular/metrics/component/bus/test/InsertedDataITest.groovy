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


package org.hawkular.metrics.component.bus.test

import com.google.common.base.Charsets
import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.hawkular.bus.common.ConnectionContextFactory
import org.hawkular.bus.common.Endpoint
import org.hawkular.bus.common.MessageProcessor
import org.hawkular.bus.common.consumer.BasicMessageListener
import org.hawkular.bus.common.consumer.ConsumerConnectionContext
import org.hawkular.metrics.component.publish.AvailDataMessage
import org.hawkular.metrics.component.publish.MetricDataMessage
import org.junit.After
import org.junit.BeforeClass
import org.junit.Test

import javax.jms.ConnectionFactory
import javax.naming.Context
import javax.naming.InitialContext
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import static java.util.concurrent.TimeUnit.MINUTES
import static org.hawkular.bus.common.Endpoint.Type.TOPIC
import static org.junit.Assert.*

/**
 * @author Thomas Segismont
 */
class InsertedDataITest {
  static baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
  static final String TENANT_PREFIX = UUID.randomUUID().toString()
  static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0)
  static String tenantHeaderName = "Hawkular-Tenant";
  static RESTClient hawkularMetrics
  static defaultFailureHandler
  static final double DELTA = 0.001

  static final String DEFAULT_CONNECTION_FACTORY = "jms/RemoteConnectionFactory";
  static final String INITIAL_CONTEXT_FACTORY = "org.jboss.naming.remote.client.InitialContextFactory";
  static final String PROVIDER_URL = System.getProperty('hawkular-metrics.remoting.url') ?: 'http-remoting://localhost:8080';
  static ConnectionFactory connectionFactory;

  @BeforeClass
  static void initClient() {
    println "Trying to use this provider URL: ${PROVIDER_URL}"
    Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
    env.put(Context.PROVIDER_URL, System.getProperty(Context.PROVIDER_URL, PROVIDER_URL));
    Context context = new InitialContext(env);
    String connectionFactoryString = System.getProperty("connection.factory", DEFAULT_CONNECTION_FACTORY);
    connectionFactory = (ConnectionFactory) context.lookup(connectionFactoryString);

    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)
    defaultFailureHandler = hawkularMetrics.handler.failure
    hawkularMetrics.handler.failure = { resp ->
      def msg = "Got error response: ${resp.statusLine}"
      if (resp.entity != null && resp.entity.contentLength != 0) {
        def baos = new ByteArrayOutputStream()
        resp.entity.writeTo(baos)
        def entity = new String(baos.toByteArray(), Charsets.UTF_8)
        msg = """${msg}
=== Response body
${entity}
===
"""
      }
      System.err.println(msg)
      return resp
    }
  }

  static String nextTenantId() {
    return "T${TENANT_PREFIX}${TENANT_ID_COUNTER.incrementAndGet()}"
  }

  def tenantId = nextTenantId()

  def consumerFactory = new ConnectionContextFactory(connectionFactory)
  ConsumerConnectionContext consumerContext
  def messageProcessor = new MessageProcessor()

  @After
  void tearDown() {
    consumerContext?.close()
    consumerFactory?.close()
  }

  @Test
  void testAvailData() {
    def endpoint = new Endpoint(TOPIC, 'HawkularAvailData')
    consumerContext = consumerFactory.createConsumerConnectionContext(endpoint)
    consumerContext.setSession(connectionFactory.createConnection().createSession())

    def latch = new CountDownLatch(1)
    AvailDataMessage actual = null
    messageProcessor.listen(consumerContext, new BasicMessageListener<AvailDataMessage>() {
      @Override
      void onBasicMessage(AvailDataMessage message) {
        actual = message
        latch.countDown()
      }
    })

    def metricName = 'test', timestamp = 13, value = 'UP'

    def response = hawkularMetrics.post(path: 'availability/data', body: [
        [
            id  : metricName,
            data: [[timestamp: timestamp, value: value]]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)


    assertTrue('No message received', latch.await(1, MINUTES))

    def data = actual?.availData?.data
    assertNotNull(data)
    assertEquals(1, data.size())

    def avail = data[0]
    assertEquals(tenantId, avail.tenantId)
    assertEquals(metricName, avail.id)
    assertEquals(timestamp, avail.timestamp)
    assertEquals(value, avail.avail)
  }

  @Test
  void testNumericData() {
    def endpoint = new Endpoint(TOPIC, 'HawkularMetricData')
    consumerContext = consumerFactory.createConsumerConnectionContext(endpoint)

    def latch = new CountDownLatch(1)
    MetricDataMessage actual = null
    messageProcessor.listen(consumerContext, new BasicMessageListener<MetricDataMessage>() {
      @Override
      void onBasicMessage(MetricDataMessage message) {
        actual = message
        latch.countDown()
      }
    })

    def metricName = 'test', timestamp = 13, value = 15.3

    def response = hawkularMetrics.post(path: 'gauges/data', body: [
        [
            id  : metricName,
            data: [[timestamp: timestamp, value: value]]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)


    assertTrue('No message received', latch.await(1, MINUTES))

    def data = actual?.metricData?.data
    assertNotNull(data)
    assertEquals(tenantId, actual.metricData.tenantId)
    assertEquals(1, data.size())

    def numeric = data[0]
    assertEquals(metricName, numeric.source)
    assertEquals(timestamp, numeric.timestamp)
    assertEquals(value, numeric.value, DELTA)
  }
}
