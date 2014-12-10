package org.rhq.metrics.impl.cassandra;

import java.util.List;

import com.google.common.base.Function;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import org.rhq.metrics.core.MetricData;

/**
 * @author John Sanda
 */
public class ComputeTTL<T extends MetricData> implements Function<List<T>, List<T>> {

    private int originalTTL;

    public ComputeTTL(int originalTTL) {
        this.originalTTL = originalTTL;
    }

    @Override
    public List<T> apply(List<T> data) {
        for (T d : data) {
            Duration duration = new Duration(DateTime.now().minus(d.getWriteTime()).getMillis());
            d.setTTL(originalTTL - duration.toStandardSeconds().getSeconds());
        }
        return data;
    }

}
