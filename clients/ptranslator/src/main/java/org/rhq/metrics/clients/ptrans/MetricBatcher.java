package org.rhq.metrics.clients.ptrans;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AttributeKey;

import org.rhq.metrics.client.common.SingleMetric;

/**
 * Batch several individual {@link org.rhq.metrics.client.common.SingleMetric} objects in a batch of
 * {@link MetricBatcher#minimumBatchSize} items to reduce backend communication overhead.
 *
 * @author Heiko W. Rupp
 */
public class MetricBatcher extends MessageToMessageDecoder<SingleMetric> {

    private int minimumBatchSize;

    /**
     * Create a batcher with the passed batch size
     * @param subKey Identification of the metrics of this batcher
     * @param minimumBatchSize Size of batches. If the number is less than 1, then 1 is used.
     */
    public MetricBatcher(String subKey, int minimumBatchSize) {
        this.minimumBatchSize = minimumBatchSize;
        if (this.minimumBatchSize<1) {
            this.minimumBatchSize=1;
        }
        cacheKey = AttributeKey.valueOf("cachedMetrics."+subKey);
    }

    AttributeKey<List<SingleMetric>> cacheKey;

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

        List<SingleMetric> cached = ctx.attr(cacheKey).get();
        if (cached==null) {
            cached = new ArrayList<>(minimumBatchSize);
            ctx.attr(cacheKey).set(cached);
        }

        if (cached.size()  >= minimumBatchSize) {
            List<SingleMetric> toForward = new ArrayList<>(cached.size()+1);
            toForward.addAll(cached);
            toForward.add(msg);
            cached.clear();
            out.add(toForward);
        } else {
            cached.add(msg);
        }
    }
}
