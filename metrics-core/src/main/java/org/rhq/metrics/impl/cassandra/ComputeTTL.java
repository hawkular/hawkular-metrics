package org.rhq.metrics.impl.cassandra;

import java.util.List;

import com.google.common.base.Function;

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
            d.setTTL((int) (originalTTL - (System.currentTimeMillis() - d.getWriteTime())));
        }
        return data;
    }

}
