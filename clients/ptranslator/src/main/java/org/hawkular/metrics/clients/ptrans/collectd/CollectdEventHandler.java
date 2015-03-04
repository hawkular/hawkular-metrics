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
package org.hawkular.metrics.clients.ptrans.collectd;

import java.util.List;
import java.util.ListIterator;

import org.hawkular.metrics.client.common.SingleMetric;
import org.hawkular.metrics.clients.ptrans.collectd.event.TimeResolution;
import org.hawkular.metrics.clients.ptrans.collectd.event.TimeSpan;
import org.hawkular.metrics.clients.ptrans.collectd.event.ValueListEvent;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

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
        List<Number> values = event.getValues();
        for (ListIterator<Number> iterator = values.listIterator(); iterator.hasNext(); ) {
            Number value = iterator.next();
            long timestamp = TimeResolution.toMillis(timeSpan);
            StringBuilder sourceBuilder = new StringBuilder(prefix);
            if (values.size() > 1) {
                sourceBuilder.append(".").append(iterator.previousIndex());
            }
            SingleMetric singleMetric = new SingleMetric(sourceBuilder.toString(), timestamp, value.doubleValue());
            out.add(singleMetric);
        }
    }
}
