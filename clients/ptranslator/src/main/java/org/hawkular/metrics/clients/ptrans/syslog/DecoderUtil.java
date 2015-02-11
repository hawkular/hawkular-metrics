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
package org.hawkular.metrics.clients.ptrans.syslog;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hawkular.metrics.client.common.SingleMetric;

/**
 * Do the actual decoding of the syslog line.
 * The expected payload format is
 * in the form "type=metric thread.count=5 thread.active=2 heap.permgen.size=25000000"
 *
 * Also supports statsd key=value|type format
 *
 * TODO Needs to support more formats.
 *
 * @author Heiko W. Rupp
 */
public class DecoderUtil {

    private static final Logger logger = LoggerFactory.getLogger(DecoderUtil.class);
    private static Pattern statsDPattern = Pattern.compile("([A-Za-z\\.]+):([0-9\\.]+)\\|[a-z]");

    public static void decodeTheBuffer(ByteBuf data, List<Object> out) {

        if (data.readableBytes()<1){
            return ; // Nothing to do
        }

        String s = data.toString(CharsetUtil.UTF_8).trim();

        Matcher matcher = statsDPattern.matcher(s);
        if (matcher.matches()) {
            // StatsD packet
            String source = matcher.group(1);
            String val = matcher.group(2);
            List<SingleMetric> metrics = new ArrayList<>(1);
            SingleMetric metric = new SingleMetric(source,System.currentTimeMillis(),Double.valueOf(val));
            metrics.add(metric);
            return;
        }

        // Not statsD, so consider syslog. But first check if the message has the right format
        if (!s.contains("type=metric")) {
            return;
        }

        String text = extractPayload(s);

        if (text.contains("type=metric")) {

            text = text.trim();

            long now = System.currentTimeMillis();

            String[] entries = text.split(" ");

            List<SingleMetric> metrics = new ArrayList<>(entries.length);

            String cartName=null;
            if (text.contains("cart=")) {
                int pos = text.indexOf("cart=");
                cartName = text.substring(pos+5,text.indexOf(' ',pos));
            }

            for (String entry: entries) {
                if (entry.equals("type=metric") || entry.startsWith("cart=")) {
                    continue;
                }
                String[] keyVal = entry.split("=");
                double value = 0;
                try {
                    value = Double.parseDouble(keyVal[1]);
                    String source = keyVal[0];
                    if (cartName!=null) {
                        source = cartName + "." + source;
                    }
                    SingleMetric metric = new SingleMetric(source,now, value);
                    metrics.add(metric);
                } catch (NumberFormatException e) {
                    if (logger.isTraceEnabled()) {
                        logger.debug("Unknown number format for " + entry + ", skipping");
                    }
                }
            }
            out.add(metrics);
        }
    }

    /**
     * Skips over the header fields of a syslog message and extracts the payload
     * @param s Raw message (a line in syslog)
     * @return The payload part
     */
    private static String extractPayload(String s) {

        int i = s.indexOf('>')+1;
        if (s.indexOf(' ',i)==i+3) {
            // Old date in old syslog has 3 chars month and then a space
            i = s.indexOf(' ', i + 5); // Day
            i = s.indexOf(' ', i+1); // Date
            i = s.indexOf(':', i+1); // job etc. TODO we may need that
            return s.substring(i+1);
        }
        else {
            // New date in syslog
            i = s.indexOf(' ',i+1); // Skip over date
            i = s.indexOf(':',i)+1; // Job etc. TODO we may need that
            return s.substring(i);
        }

    }
}
