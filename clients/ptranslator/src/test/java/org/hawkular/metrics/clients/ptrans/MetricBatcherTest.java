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
package org.hawkular.metrics.clients.ptrans;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.hawkular.metrics.client.common.SingleMetric;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @author Thomas Segismont
 */
public class MetricBatcherTest {

    @Rule
    public final Timeout globalTimeout = new Timeout(1, MINUTES);
    @Rule
    public final TestName testName = new TestName();

    @Test
    public void batcherShouldNotForwardMetricsIfSizeIsNotReachedAndChannelNotIdle() {
        int batchSize = 10;
        MetricBatcher metricBatcher = new MetricBatcher(testName.getMethodName(), batchSize);
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(metricBatcher);

        int metricsCount = batchSize - 5;
        for (int i = 0; i < metricsCount; i++) {
            embeddedChannel.writeInbound(
                    new SingleMetric(
                            testName.getMethodName(),
                            System.currentTimeMillis() - i,
                            13.0d + i
                    )
            );
        }

        assertNull("No batch should be forwarded", embeddedChannel.readInbound());
    }

    @Test
    public void batcherShouldForwardMetricsIfSizeIsReached() {
        int batchSize = 10;
        MetricBatcher metricBatcher = new MetricBatcher(testName.getMethodName(), batchSize);
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(metricBatcher);

        int expectedBatchesCount = 2;
        int metricsCount = expectedBatchesCount * batchSize + 5;
        for (int i = 0; i < metricsCount; i++) {
            embeddedChannel.writeInbound(
                    new SingleMetric(
                            testName.getMethodName(),
                            System.currentTimeMillis() - i,
                            13.0d + i
                    )
            );
        }

        for (int i = 0; i < expectedBatchesCount; i++) {
            checkBatchOutput(embeddedChannel, batchSize);
        }
        assertNull("Unexpected batch forwarded", embeddedChannel.readInbound());
    }

    private void checkBatchOutput(EmbeddedChannel embeddedChannel, int batchSize) {
        Object object = embeddedChannel.readInbound();
        assertNotNull("Expected a forwarded batch", object);
        assertThat(object, instanceOf(List.class));
        List<?> list = (List<?>) object;
        assertEquals("Unexpected batch size", batchSize, list.size());
        for (Object item : list) {
            assertThat(item, instanceOf(SingleMetric.class));
        }
    }

    @Test
    public void batcherShouldForwardMetricsIfSizeIsNotReachedButChannelIsIdle() {
        int batchSize = 10;
        MetricBatcher metricBatcher = new MetricBatcher(testName.getMethodName(), batchSize);
        EmbeddedChannel embeddedChannel = new EmbeddedChannel(metricBatcher);

        int metricsCount = batchSize - 5;
        for (int i = 0; i < metricsCount; i++) {
            embeddedChannel.writeInbound(
                    new SingleMetric(
                            testName.getMethodName(),
                            System.currentTimeMillis() - i,
                            13.0d + i
                    )
            );
        }

        embeddedChannel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);

        checkBatchOutput(embeddedChannel, metricsCount);
        assertNull("Unexpected batch forwarded", embeddedChannel.readInbound());
    }
}