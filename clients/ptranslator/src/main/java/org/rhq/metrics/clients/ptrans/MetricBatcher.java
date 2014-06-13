package org.rhq.metrics.clients.ptrans;

import java.util.ArrayList;
import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AttributeKey;


/**
 * Batch several individual {@link SingleMetric} objects in a batch of {@link MetricBatcher#THRESHOLD}
 * items to reduce backend communication overhead.
 *
 * TODO how can we detect that Netty is going down and first send the last batch?
 *
 * @author Heiko W. Rupp
 */
public class MetricBatcher extends MessageToMessageDecoder<SingleMetric> {

    private static final int THRESHOLD = 5;
    private String subKey;

    public MetricBatcher(String subKey) {
        this.subKey = subKey;
        cacheKey = AttributeKey.valueOf("cachedMetrics"+subKey);
    }

    AttributeKey<List<SingleMetric>> cacheKey;

    @Override
    protected void decode(ChannelHandlerContext ctx, SingleMetric msg, List<Object> out) throws Exception {

        List<SingleMetric> cached = ctx.attr(cacheKey).get();
        if (cached==null) {
            cached = new ArrayList<>(5);
            ctx.attr(cacheKey).set(cached);
        }

        if (cached.size()  >= THRESHOLD) {
            List<SingleMetric> toForward = new ArrayList<>(6);
            toForward.addAll(cached);
            toForward.add(msg);
            cached.clear();
            out.add(toForward);
        } else {
            cached.add(msg);
        }
    }
}
