package org.rhq.metrics.clients.ptrans.collectd;

import static io.netty.channel.ChannelHandler.Sharable;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import org.rhq.metrics.client.common.SingleMetric;
import org.rhq.metrics.netty.collectd.event.TimeResolution;
import org.rhq.metrics.netty.collectd.event.TimeSpan;
import org.rhq.metrics.netty.collectd.event.ValueListEvent;

/**
 * @author Thomas Segismont
 */
@Sharable
public class CollectdEventHandler extends MessageToMessageDecoder<ValueListEvent> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ValueListEvent event, List<Object> out) throws Exception {
        StringBuilder prefixBuilder = new StringBuilder("collectd.").append(event.getHost()).append(".")
            .append(event.getPluginName());
        String pluginInstance = event.getPluginInstance();
        if (pluginInstance != null && pluginInstance.length() > 0) {
            prefixBuilder.append(".").append(pluginInstance);
        }
        prefixBuilder.append(".").append(event.getTypeName());
        String typeInstance = event.getTypeInstance();
        if (typeInstance != null && typeInstance.length() > 0) {
            prefixBuilder.append(".").append(typeInstance);
        }
        String prefix = prefixBuilder.toString();
        TimeSpan timeSpan = event.getTimestamp();
        Number[] values = event.getValues();
        for (int i = 0; i < values.length; i++) {
            Number value = values[i];
            long timestamp = TimeResolution.toMillis(timeSpan);
            StringBuilder sourceBuilder = new StringBuilder(prefix);
            if (values.length > 1) {
                sourceBuilder.append(".").append(i);
            }
            SingleMetric singleMetric = new SingleMetric(sourceBuilder.toString(), timestamp, value.doubleValue());
            out.add(singleMetric);
        }
    }
}
