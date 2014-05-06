package org.rhq.metrics.clients.syslogRest;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Do the actual decoding of the syslog line.
 *
 * Needs to support more formats.
 *
 * @author Heiko W. Rupp
 */
public class DecoderUtil {

    private static final Logger logger = LoggerFactory.getLogger(DecoderUtil.class);

    public static void decodeTheBuffer(ByteBuf data, List<Object> out) {

        if (data.readableBytes()<1){
            return ; // Nothing to do
        }

        String s = data.toString(CharsetUtil.UTF_8);
        if (logger.isDebugEnabled()) {
            logger.debug("Incoming: >>" + s + "<<");
        }
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

            List<SyslogMetricEvent> eventList = new ArrayList<>(entries.length);

            for (String entry: entries) {
                if (entry.equals("type=metric")) {
                    continue;
                }
                String[] keyVal = entry.split("=");
                double value = 0;
                try {
                    value = Double.parseDouble(keyVal[1]);
                } catch (NumberFormatException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }
                SyslogMetricEvent event = new SyslogMetricEvent(keyVal[0],now, value);
                eventList.add(event);
            }
            out.add(eventList);
        }
    }
}
