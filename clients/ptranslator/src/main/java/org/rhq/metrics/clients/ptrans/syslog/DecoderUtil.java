package org.rhq.metrics.clients.ptrans.syslog;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.rhq.metrics.clients.ptrans.SingleMetric;

/**
 * Do the actual decoding of the syslog line.
 * The expected payload format is
 * in the form "type=metric thread.count=5 thread.active=2 heap.permgen.size=25000000"
 *
 * TODO Needs to support more formats.
 *
 * @author Heiko W. Rupp
 */
public class DecoderUtil {

    private static final Logger logger = LoggerFactory.getLogger(DecoderUtil.class);

    public static void decodeTheBuffer(ByteBuf data, List<Object> out) {

        if (data.readableBytes()<1){
            return ; // Nothing to do
        }

        String s = data.toString(CharsetUtil.UTF_8).trim();
        if (!s.contains("type=metric")) {
            return;
        }

        int i = data.indexOf(0, data.readableBytes(),
            (byte) '>');
        ByteBuf buf = data.slice(i + 1, data.readableBytes());
        ByteBuf dateBuf = data.slice(i+1,i+13);

        i = data.indexOf(i+16,data.readableBytes(),(byte)':');
        buf = data.slice(i+2,data.readableBytes());
        String text = buf.toString(CharsetUtil.UTF_8);

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
}
