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

import static java.time.ZoneOffset.UTC;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.metrics.sysconfig.Configuration;
import org.hawkular.metrics.sysconfig.ConfigurationService;
import org.jboss.logging.Logger;

import rx.Completable;
import rx.functions.Func1;

/**
 * This job creates (if required) all the necessary temp tables to Cassandra
 *
 * @author Michael Burman
 */
public class TempTableCreator implements Func1<JobDetails, Completable> {

    private MetricsService service;

    private static Logger logger = Logger.getLogger(TempTableCreator.class);

    public static final String JOB_NAME = "TEMP_TABLE_CREATOR";
    public static final String CONFIG_ID = "org.hawkular.metrics.jobs." + JOB_NAME;
    public static final String FORWARD_TIME = CONFIG_ID + "buffer.tables.time.forward.duration";

    public static final Duration DEFAULT_FORWARD_TIME = Duration.of(1, ChronoUnit.DAYS);

    private Duration forwardTime;

    public TempTableCreator(MetricsService metricsService, ConfigurationService configurationService) {
        service = metricsService;
        Configuration configuration = configurationService.load(CONFIG_ID).toSingle().toBlocking().value();

        if (configuration.get(FORWARD_TIME) != null) {
            forwardTime = java.time.Duration.parse(configuration.get(FORWARD_TIME));
        } else {
            forwardTime = DEFAULT_FORWARD_TIME;
        }
    }

    @Override
    public Completable call(JobDetails jobDetails) {

        Trigger trigger = jobDetails.getTrigger();

        ZonedDateTime currentBlock = ZonedDateTime.ofInstant(Instant.ofEpochMilli(trigger.getTriggerTime()), UTC)
                .with(DateTimeService.startOfPreviousEvenHour());

        ZonedDateTime lastMaintainedBlock = currentBlock.plus(forwardTime);

        return service.verifyAndCreateTempTables(currentBlock, lastMaintainedBlock)
                .doOnCompleted(() -> logger.debugf("Temporary tables are valid until %s",
                        lastMaintainedBlock.toString()));
    }
}
