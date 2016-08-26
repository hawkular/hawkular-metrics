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

import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.core.service.cache.CacheService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;

import com.google.common.base.Stopwatch;

import rx.Completable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class ComputeRollups implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(ComputeRollups.class);

    public static final String JOB_NAME = "COMPUTE_ROLLUPS";

    private CacheService cacheService;

    public ComputeRollups(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public Completable call(JobDetails details) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DateTime time = new DateTime(details.getTrigger().getTriggerTime());
        long end = details.getTrigger().getTriggerTime();
        long start = new DateTime(end).minusMinutes(1).getMillis();

        logger.info("Preparing to compute roll ups for time range {start=" + start + ", end=" + end + "}");
        logger.info("Total number of cache entries: " + cacheService.getRawDataCache().getStats()
                .getTotalNumberOfEntries());

        return cacheService.removeFromRawDataCache(start).doOnCompleted(() -> {
            stopwatch.stop();
            logger.info("Finished rollups in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        });
    }
}
