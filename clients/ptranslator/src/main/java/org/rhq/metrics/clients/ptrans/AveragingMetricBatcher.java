package org.rhq.metrics.clients.ptrans;

import java.util.ArrayList;
import java.util.List;

/**
 * This batches takes the recorded values and creates
 * an average that is then forwarded.
 * @author Heiko W. Rupp
 */
public class AveragingMetricBatcher extends MetricBatcher {

    public AveragingMetricBatcher(String subKey) {
        super(subKey, 5);

    }

    @Override
    protected List<SingleMetric> process(List<SingleMetric> toForward) {
        List<SingleMetric> ret = new ArrayList<>(1);

        if (toForward.isEmpty()) {
            return ret;
        }

        long timestamp = toForward.get(0).getTimestamp();

        Double sum = 0.0;

        for (SingleMetric metric : toForward) {
            sum += metric.getValue();
        }
//        SingleMetric out = new SingleMetric(,timestamp,sum/toForward.size());
//        ret.add(out);

        return ret;
    }
}
