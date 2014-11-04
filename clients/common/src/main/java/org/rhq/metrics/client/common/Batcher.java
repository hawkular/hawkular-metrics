package org.rhq.metrics.client.common;

import java.util.Collection;
import java.util.Iterator;

/**
 * Just a helper
 * @author Heiko W. Rupp
 */
public class Batcher {

    private Batcher() {
        // Utility class
    }

    /**
     * Translate the passed collection of metrics into a JSON representation
     * @param metrics a Collection of metrics to translate
     * @return String as JSON representation of the Metrics.
     */
    public static String metricListToJson(final Collection<SingleMetric> metrics) {
            StringBuilder builder = new StringBuilder("[");
            Iterator<SingleMetric> iter = metrics.iterator();
            while (iter.hasNext()) {
                SingleMetric event = iter.next();
                builder.append(event.toJson());
                if (iter.hasNext()) {
                    builder.append(',');
                }
            }
            builder.append(']');

            return builder.toString();
        }

}
