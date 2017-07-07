/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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

import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.functions.Func1;

/**
 * @author Michael Burman
 */
public class TempDataCompressor implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(TempDataCompressor.class);

    public static final String JOB_NAME = "TEMP_DATA_COMPRESSOR";
    public static final String CONFIG_ID = JobsServiceImpl.CONFIG_PREFIX + JOB_NAME;
    public static final String CONFIG_PAGE_SIZE = "page-size";
    public static final String CONFIG_MAX_READ_CONCURRENCY = "concurrency.read.max";

    private static final int DEFAULT_PAGE_SIZE = 1000;
    private static final int DEFAULT_READ_CONCURRENCY = 2;

    private MetricsService metricsService;

    private int pageSize;
    private boolean enabled;
    private int maxReadConcurrency = DEFAULT_READ_CONCURRENCY;

    public TempDataCompressor(MetricsService service, ConfigurationService configurationService) {
        metricsService = service;
        Configuration configuration = configurationService.load(CONFIG_ID).toSingle().toBlocking().value();
        if (configuration.get(CONFIG_PAGE_SIZE) == null) {
            pageSize = DEFAULT_PAGE_SIZE;
        } else {
            pageSize = Integer.parseInt(configuration.get(CONFIG_PAGE_SIZE));
        }

        if(configuration.get(CONFIG_MAX_READ_CONCURRENCY) != null) {
            maxReadConcurrency = Integer.parseInt(configuration.get(CONFIG_MAX_READ_CONCURRENCY));
        }

        String enabledConfig = configuration.get("enabled", "true");
        enabled = Boolean.parseBoolean(enabledConfig);
        logger.debugf("Job enabled? %b", enabled);
    }

    @Override
    public Completable call(JobDetails jobDetails) {
        Duration runtimeBlockSize = Duration.standardHours(2);

        Trigger trigger = jobDetails.getTrigger();
        DateTime timeSliceInclusive = new DateTime(trigger.getTriggerTime(), DateTimeZone.UTC).minus(runtimeBlockSize);

        // Rewind to previous timeslice
        DateTime timeSliceStart = DateTimeService.getTimeSlice(timeSliceInclusive, runtimeBlockSize);
        long startOfSlice = timeSliceStart.getMillis();

        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.infof("Starting to process temp table for starting time of %s", timeSliceStart.toString());

        // TODO Optimization - new worker per token - use parallelism in Cassandra (with configured parallelism)
        return metricsService.compressBlock(startOfSlice, pageSize, maxReadConcurrency)
                .doOnCompleted(() -> {
                    stopwatch.stop();
                    logger.info("Finished processing data in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
                            " ms");
                });
    }
}
