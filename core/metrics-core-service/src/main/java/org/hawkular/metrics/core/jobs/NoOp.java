/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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

import java.util.Random;

import org.hawkular.metrics.scheduler.api.JobDetails;
import org.jboss.logging.Logger;

import rx.Completable;
import rx.functions.Func1;

/**
 * @author jsanda
 */
public class NoOp implements Func1<JobDetails, Completable> {

    private static Logger logger = Logger.getLogger(NoOp.class);

    private Random random = new Random();

    @Override
    public Completable call(JobDetails details) {
        try {
            logger.info("Starting job execution");
            long timeout = Math.abs(random.nextLong()) % 10000L;
            Thread.sleep(timeout);
            logger.info("Finished job execution");
        } catch (InterruptedException e) {
            logger.warn("Interrupt");
            return Completable.error(e);
        }
        return Completable.complete();
    }
}
