package org.rhq.metrics.core;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

import rx.Observable;

/**
 * @author John Sanda
 */
public interface MetricsServiceRx {

    ListenableFuture<Void> createTenant(Tenant tenant);

    ListenableFuture<Void> addNumericData(List<NumericMetric2> metrics);

    Observable<NumericMetric2> findNumericData(NumericMetric2 metric, long start, long end);

}
