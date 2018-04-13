/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
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
    public static final String BLOCK_SIZE = "compression.block.size";
    public static final String TARGET_TIME = "compression.time.target";
    public static final String CONFIG_ID = "org.hawkular.metrics.jobs." + JOB_NAME;
    public static final Duration DEFAULT_BLOCK_SIZE = Duration.standardHours(2);

    private static final int DEFAULT_PAGE_SIZE = 1000;

    private MetricsService metricsService;

    private int pageSize;
    private boolean enabled;
    private Duration blockSize;

    public CompressData(MetricsService service, ConfigurationService configurationService) {
        metricsService = service;
        Configuration configuration = configurationService.load(CONFIG_ID).toSingle().toBlocking().value();
        if (configuration.get("page-size") == null) {
            pageSize = DEFAULT_PAGE_SIZE;
        } else {
            pageSize = Integer.parseInt(configuration.get("page-size"));
        }

        if (configuration.get(BLOCK_SIZE) != null) {
            java.time.Duration parsedDuration = java.time.Duration.parse(configuration.get(BLOCK_SIZE));
            blockSize = Duration.millis(parsedDuration.toMillis());
        } else {
            blockSize = DEFAULT_BLOCK_SIZE;
        }

        String enabledConfig = configuration.get("enabled", "true");
        enabled = Boolean.parseBoolean(enabledConfig);
        logger.debugf("Job enabled? %b", enabled);
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        Duration runtimeBlockSize = blockSize;
        DateTime timeSliceInclusive;

        Trigger trigger = jobDetails.getTrigger();
        if(trigger instanceof RepeatingTrigger) {
            if (!enabled) {
                return Completable.complete();
            }
            timeSliceInclusive = new DateTime(trigger.getTriggerTime(), DateTimeZone.UTC).minus(runtimeBlockSize);
        } else {
            if(jobDetails.getParameters().containsKey(TARGET_TIME)) {
                // DateTime parsing fails without casting to Long first
                Long parsedMillis = Long.valueOf(jobDetails.getParameters().get(TARGET_TIME));
                timeSliceInclusive = new DateTime(parsedMillis, DateTimeZone.UTC);
            } else {
                logger.error("Missing " + TARGET_TIME + " parameter from manual execution of " + JOB_NAME + " job");
                return Completable.complete();
            }

            if(jobDetails.getParameters().containsKey(BLOCK_SIZE)) {
                java.time.Duration parsedDuration =
                        java.time.Duration.parse(jobDetails.getParameters().get(BLOCK_SIZE));
                runtimeBlockSize = Duration.millis(parsedDuration.toMillis());
            }
        }

        // Rewind to previous timeslice
        DateTime timeSliceStart = DateTimeService.getTimeSlice(timeSliceInclusive, runtimeBlockSize);
        long startOfSlice = timeSliceStart.getMillis();
        long endOfSlice = timeSliceStart.plus(runtimeBlockSize).getMillis() - 1;

        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting compression of timestamps (UTC) between " + startOfSlice + " - " + endOfSlice);

        Observable<? extends MetricId<?>> metricIds = metricsService.findAllMetricIdentifiers()
                .filter(m -> (m.getType() == GAUGE || m.getType() == COUNTER || m.getType() == AVAILABILITY));

        // Fetch all partition keys and compress the previous timeSlice
        // TODO Optimization - new worker per token - use parallelism in Cassandra (with configured parallelism)
        return metricsService.compressBlock(metricIds, startOfSlice, endOfSlice, pageSize)
                .doOnError(t -> logger.warn("Failed to compress data", t))
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    logger.info("Finished compressing data in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
                            " ms");
                });
    }
}
