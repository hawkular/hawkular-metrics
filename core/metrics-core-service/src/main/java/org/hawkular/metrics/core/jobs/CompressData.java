/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.jobs;

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.Observable;
import rx.functions.Func1;

/**
 * @author Michael Burman
 */
public class CompressData implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(CompressData.class);

    public static final String JOB_NAME = "COMPRESS_DATA";
    public static final String ENABLED_CONFIG = "compression.enabled";
    public static final String BLOCK_SIZE = "compression.block.size";

    // TODO Make this configurable by reading BLOCK_SIZE from ConfigurationService
    public static final Duration DEFAULT_BLOCK_SIZE = Duration.standardHours(2);

    private MetricsService metricsService;

    public CompressData(MetricsService service) {
        metricsService = service;
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        // Rewind to previous timeslice
        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting execution");
        long previousBlock = DateTimeService.getTimeSlice(new DateTime(jobDetails.getTrigger().getTriggerTime(),
                DateTimeZone.UTC).minus(DEFAULT_BLOCK_SIZE), DEFAULT_BLOCK_SIZE).getMillis();

        Observable<? extends MetricId<?>> metricIds = metricsService.findAllMetrics()
                .map(Metric::getMetricId)
                .filter(m -> (m.getType() == GAUGE || m.getType() == COUNTER || m.getType() == AVAILABILITY));

        // Fetch all partition keys and compress the previous timeSlice
        return Completable.fromObservable(
                metricsService.compressBlock(metricIds, previousBlock)
                        .doOnError(t -> logger.warn("Failed to compress data", t))
                        .doOnCompleted(() -> {
                            stopwatch.stop();
                            logger.info("Finished compressing data in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
                                    " ms");
                        })
        );

        // TODO Optimization - new worker per token - use parallelism in Cassandra
        // Configure the amount of parallelism?
    }
}
