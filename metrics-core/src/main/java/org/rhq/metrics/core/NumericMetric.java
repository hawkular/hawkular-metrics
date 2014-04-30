package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public interface NumericMetric {

    String getBucket();

    String getId();

    Double getMin();

    Double getMax();

    Double getAvg();

    long getTimestamp();

}
