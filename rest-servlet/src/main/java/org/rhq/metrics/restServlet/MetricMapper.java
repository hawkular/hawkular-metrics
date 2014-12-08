package org.rhq.metrics.restServlet;

import com.google.common.base.Function;

import org.rhq.metrics.core.NumericMetric2;

/**
 * @author John Sanda
 */
public abstract class MetricMapper<T> implements Function<NumericMetric2, T> {

    @Override
    public T apply(NumericMetric2 metric) {
        if (metric == null) {
            throw new NoResultsException();
        }
        return doApply(metric);
    }

    abstract T doApply(NumericMetric2 metric);
}
