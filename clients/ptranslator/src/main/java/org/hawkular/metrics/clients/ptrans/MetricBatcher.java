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

import java.util.ArrayList;
import java.util.List;

import org.hawkular.metrics.client.common.SingleMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * Batch several individual {@link org.hawkular.metrics.client.common.SingleMetric} objects to reduce backend
 * communication overhead.
 * <p>
 * When creating the channel pipeline, an instance of
 * {@link io.netty.handler.timeout.IdleStateHandler} should be added just before an instance of this class. Metric
 * batcher instances handle read {@link io.netty.handler.timeout.IdleStateEvent}s.<br>
 * When such an event is captured, the batch is forwarded even if the size limit is not reached. Consequently, no
 * metric can stay in the cache several minutes before being sent to the server.
 * </p>
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 * @see io.netty.handler.timeout.IdleStateHandler
 */
public class MetricBatcher extends MessageToMessageDecoder<SingleMetric> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricBatcher.class);

    private final int minimumBatchSize;
    private final AttributeKey<List<SingleMetric>> cacheKey;

    /**
     * Create a batcher with the passed batch size
     * @param subKey Identification of the metrics of this batcher
     * @param minimumBatchSize Size of batches. If the number is less than 1, then 1 is used.
     */
    public MetricBatcher(String subKey, int minimumBatchSize) {
        this.minimumBatchSize = Math.max(1, minimumBatchSize);
        cacheKey = AttributeKey.valueOf("cachedMetrics."+subKey);
    }

    /**
     * Batch up incoming SingleMetric messages. If the #minimumBatchSize is not yet reached, the messages are stored
     * locally. Otherwise the list of messages will be forwarded to the next handler.
     * This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToMessageDecoder} belongs to
     * @param msg           the SingleMetric to be batched up
     * @param out           the {@link List} to which decoded messages should be added if the batch size is reached
     * @throws Exception    is thrown if an error occurs
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, SingleMetric msg, List<Object> out) throws Exception {
        LOG.trace("Incoming metric for key '{}'", cacheKey.name());
        Attribute<List<SingleMetric>> cache = ctx.attr(cacheKey);
        List<SingleMetric> batchList = cache.get();
        if (batchList == null) {
            LOG.trace("Creating new batch list for key '{}'", cacheKey.name());
            batchList = new ArrayList<>(minimumBatchSize);
            cache.set(batchList);
        }
        batchList.add(msg);
        if (batchList.size() >= minimumBatchSize) {
            LOG.trace("Batch size limit '{}' reached for key '{}'", minimumBatchSize, cacheKey.name());
            cache.remove();
            out.add(batchList);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (!(evt instanceof IdleStateEvent)) {
            LOG.trace("Dropping unhandled event '{}' for key '{}'", evt, cacheKey.name());
            return;
        }
        IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
        if (idleStateEvent != IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT) {
            LOG.trace("Dropping event, expecting FIRST_READER_IDLE_STATE_EVENT for key '{}'", cacheKey.name());
            return;
        }
        List<SingleMetric> batchList = ctx.attr(cacheKey).getAndRemove();
        if (batchList != null && !batchList.isEmpty()) {
            LOG.trace("Batch delay reached for key '{}', forwarding {} metrics", cacheKey.name(), batchList.size());
            ctx.fireChannelRead(batchList);
        }
    }
}
