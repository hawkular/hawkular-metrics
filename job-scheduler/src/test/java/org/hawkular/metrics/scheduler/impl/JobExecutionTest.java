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
package org.hawkular.metrics.scheduler.impl;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RepeatingTrigger;
import org.hawkular.metrics.scheduler.api.SingleExecutionTrigger;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.collect.ImmutableMap;

import rx.Observable;
import rx.internal.schedulers.SchedulerLifecycle;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class JobExecutionTest extends JobSchedulerTest {

    private static Logger logger = Logger.getLogger(JobExecutionTest.class);

    /**
     * Used in place of SchedulerImpl.tickScheduler so that we can control the clock.
     */
    private TestScheduler tickScheduler;

    /**
     * Publishes notifications when the job scheduler finishes executing jobs for a time slice (or if there are no
     * jobs to execute). This a test hook that allows to verify the state of things incrementally as work is completed.
     */
    private PublishSubject<Date> finishedTimeSlices;

    /**
     * Tests use these as a synchronization mechanism. We want to verify state when work finishes. Tests can set
     * finishedTimeSlice to indicate at what point verification should happen. timeSliceFinished gets decremented when
     * finishedTimeSlice is set and when the job scheduler finishes the specified time slice. These are used in
     * conjunction with {@link #waitForTimeSliceToFinish(DateTime, long, TimeUnit)}.
     */
    private DateTime finishedTimeSlice;
    private CountDownLatch timeSliceFinished;

    private PreparedStatement insertJob;
    private PreparedStatement updateJobQueue;

    @BeforeClass
    public void initClass() {
        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(currentMinute().getMillis(), TimeUnit.MILLISECONDS);

        DateTimeService.now = () -> new DateTime(tickScheduler.now());

        finishedTimeSlices = PublishSubject.create();

        insertJob = session.prepare("INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        updateJobQueue = session.prepare("INSERT INTO jobs_status (time_slice, job_id) VALUES (?, ?)");

        setActiveQueue(new DateTime(tickScheduler.now()));

        jobScheduler.setTickScheduler(tickScheduler);
        jobScheduler.setTimeSlicesSubject(finishedTimeSlices);
        finishedTimeSlices.subscribe(timeSlice -> {
            logger.debug("Finished time slice [" + timeSlice + "]");

            if (finishedTimeSlice != null && timeSlice.getTime() >= finishedTimeSlice.getMillis()) {
                timeSliceFinished.countDown();
            }
        });
    }

    @BeforeMethod
    public void initTest(Method method) throws Exception {
        logger.debug("Starting [" + method.getName() + "]");

        CountDownLatch truncationFinished = new CountDownLatch(1);
        Observable<ResultSet> o1 = rxSession.execute("TRUNCATE jobs");
        Observable<ResultSet> o2 = rxSession.execute("TRUNCATE jobs_status");
        Observable<ResultSet> o3 = rxSession.execute("TRUNCATE finished_jobs_time_idx");

        Observable.merge(o1, o2, o3).subscribe(
                resultSet -> {},
                t -> fail("Truncating tables failed", t),
                truncationFinished::countDown
        );

        finishedTimeSlice = null;
        timeSliceFinished = new CountDownLatch(1);
        truncationFinished.await();

        jobScheduler.start();
    }

    @AfterMethod
    public void resetJobScheduler() {
        jobScheduler.reset(tickScheduler);
        SchedulerLifecycle computationLifecycle = (SchedulerLifecycle) Schedulers.computation();
        computationLifecycle.shutdown();
        computationLifecycle.start();
    }

    /**
     * This test runs the scheduler when no jobs are scheduled.
     */
    @Test
    public void advanceClockWhenNoJobsAreScheduled() throws Exception {
        DateTime timeSlice = new DateTime(tickScheduler.now()).plusMinutes(1);

        setActiveQueue(timeSlice);
        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);

        waitForTimeSliceToFinish(timeSlice, 5, TimeUnit.SECONDS);

        assertEquals(getActiveQueue(), timeSlice.plusMinutes(1));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
    }

    /**
     *
     * This test runs the scheduler when there is one, single execution job that completes within its scheduled time
     * slice.
     */
    @Test
    public void executeSingleExecutionJob() throws Exception {
        DateTime timeSlice = new DateTime(tickScheduler.now()).plusMinutes(1);

        Trigger trigger = new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build();
        JobDetails jobDetails = new JobDetails(UUID.randomUUID(), "Test Type", "Test Job 1", emptyMap(), trigger);
        AtomicInteger executionCountRef = new AtomicInteger();

        jobScheduler.registerJobCreator(jobDetails.getJobType(), details -> Observable.create(subscriber -> {
            logger.debug("Executing " + details);
            executionCountRef.incrementAndGet();
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        session.execute(insertJob.bind(jobDetails.getJobId(), jobDetails.getJobType(), jobDetails.getJobName(),
                jobDetails.getParameters(), SchedulerImpl.getTriggerValue(rxSession, trigger)));
        session.execute(updateJobQueue.bind(timeSlice.toDate(), jobDetails.getJobId()));

        logger.debug("Job scheduled to execute at " + new Date(trigger.getTriggerTime()));

        setActiveQueue(timeSlice);

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);

        waitForTimeSliceToFinish(timeSlice, 5, TimeUnit.SECONDS);

        assertEquals(executionCountRef.get(), 1, jobDetails + " should have been executed once");

        assertEquals(getActiveQueue(), timeSlice.plusMinutes(1));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
    }

    /**
     *
     * This test runs the scheduler when there are two, single execution jobs that complete within their scheduled time
     * slice.
     */
    @Test
    public void executeMultipleSingleExecutionJobs() throws Exception {
        DateTime timeSlice = new DateTime(tickScheduler.now()).plusMinutes(1);

        Trigger trigger = new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build();
        String jobType = "Test Type";
        Map<String, Integer> executionCounts = new HashMap<>();
        List<JobDetails> jobDetailsList = new ArrayList<>();

        logger.debug("Scheduling jobs for time slice [" + timeSlice.toLocalDateTime() + "]");

        for (int i = 0; i < 3; ++i) {
            JobDetails details = new JobDetails(UUID.randomUUID(), jobType, "Test Job " + i, emptyMap(), trigger);
            jobDetailsList.add(details);
            executionCounts.put(details.getJobName(), 0);

            session.execute(insertJob.bind(details.getJobId(), details.getJobType(), details.getJobName(),
                    details.getParameters(), SchedulerImpl.getTriggerValue(rxSession, trigger)));
            session.execute(updateJobQueue.bind(timeSlice.toDate(), details.getJobId()));
        }

        jobScheduler.registerJobCreator(jobType, details -> Observable.create(subscriber -> {
            logger.debug("Executing " + details);
            Integer count = executionCounts.get(details.getJobName());
            executionCounts.put(details.getJobName(), ++count);
            logger.debug("Execution Counts = " + executionCounts);
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        setActiveQueue(timeSlice);

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);

        waitForTimeSliceToFinish(timeSlice, 10, TimeUnit.SECONDS);

        assertEquals(executionCounts, ImmutableMap.of("Test Job 0", 1, "Test Job 1", 1, "Test Job 2", 1));
        assertEquals(getActiveQueue(), timeSlice.plusMinutes(1));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
    }

    /**
     * This test executes two single execution jobs. The first job is scheduled earlier than the second, and it does
     * not finish executing until the second job is executed.
     */
    @Test
    public void executeLongRunningSingleExecutionJob() throws Exception {
        DateTime timeSlice = new DateTime(tickScheduler.now()).plusMinutes(1);

        JobDetails job1 = new JobDetails(UUID.randomUUID(), "Long Test Job", "Long Test Job", emptyMap(),
                new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build());
        CountDownLatch job1Finished = new CountDownLatch(1);
        CountDownLatch firstTimeSliceFinished = new CountDownLatch(1);

        jobScheduler.registerJobCreator(job1.getJobType(), details -> Observable.create(subscriber -> {
            try {
                logger.debug("First time slice finished!");
                // This is to let the test know that this job has started executing and the clock can be advanced
                // accordingly.
                firstTimeSliceFinished.countDown();

                // Now we wait until the clock is advanced to the later time slice. This allows us to test the scenario
                // in which there is still is a job running that was started in an earlier time slice. The job scheduler
                // has to look for jobs to execute in later time slices.
                job1Finished.await();
            } catch (InterruptedException e) {
                subscriber.onError(e);
            }
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        JobDetails job2 = new JobDetails(UUID.randomUUID(), "Test Type", "Test Job", emptyMap(),
                new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.plusMinutes(1).getMillis()).build());

        jobScheduler.registerJobCreator(job2.getJobType(), details -> Observable.create(subscriber -> {
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        scheduleJob(job1);
        scheduleJob(job2);

        setActiveQueue(timeSlice);

        tickScheduler.advanceTimeTo(job1.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS);
        firstTimeSliceFinished.await(10, TimeUnit.SECONDS);

        tickScheduler.advanceTimeTo(job2.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS);

        job1Finished.countDown();

        waitForTimeSliceToFinish(timeSlice.plusMinutes(1), 10, TimeUnit.SECONDS);

        logger.debug("Time slice [" + timeSlice.toDate() + "] has finished.");

        assertEquals(getActiveQueue(), timeSlice.plusMinutes(2));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    @Test
    public void executeJobThatRepeatsEveryMinute() throws Exception {
        Trigger trigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(1, TimeUnit.MINUTES)
                .build();
        DateTime timeSlice = new DateTime(trigger.getTriggerTime());
        JobDetails jobDetails = new JobDetails(UUID.randomUUID(), "Repeat Test", "Repeat Test", emptyMap(), trigger);
        CountDownLatch[] timeSliceFinished = new CountDownLatch[] {new CountDownLatch(1), new CountDownLatch(1)};
        AtomicInteger executionCount = new AtomicInteger();

        jobScheduler.registerJobCreator(jobDetails.getJobType(), details -> Observable.create(subscriber -> {
            logger.debug("Executing " + jobDetails);
            timeSliceFinished[executionCount.getAndIncrement()].countDown();
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        scheduleJob(jobDetails);
        setActiveQueue(timeSlice);

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(timeSliceFinished[0].await(10, TimeUnit.SECONDS));
        waitForTimeSliceToFinish(timeSlice, 10, TimeUnit.SECONDS);

        this.timeSliceFinished = new CountDownLatch(1);
        tickScheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        assertTrue(timeSliceFinished[1].await(10, TimeUnit.SECONDS));
        waitForTimeSliceToFinish(timeSlice.plusMinutes(1), 10, TimeUnit.SECONDS);

        assertEquals(getActiveQueue(), timeSlice.plusMinutes(2));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    @Test
    public void executeMultipleRepeatingJobs() throws Exception {
        Trigger trigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(1, TimeUnit.MINUTES)
                .build();
        DateTime timeSlice = new DateTime(trigger.getTriggerTime());
        String jobType = "Repeat Test";
        String jobNamePrefix = "Repeat Test Job ";
        Map<String, List<DateTime>> executions = new HashMap<>();
        CountDownLatch[] jobsExecuted = new CountDownLatch[] {new CountDownLatch(3), new CountDownLatch(3)};
        AtomicInteger executionCount = new AtomicInteger();

        for (int i = 0; i < 3; ++i) {
            JobDetails details = new JobDetails(UUID.randomUUID(), jobType, jobNamePrefix + i, emptyMap(), trigger);
            executions.put(details.getJobName(), new ArrayList<>());
            scheduleJob(details);
        }

        jobScheduler.registerJobCreator(jobType, jobDetails -> Observable.create(subscriber -> {
            logger.debug("Executing " + jobDetails);
            List<DateTime> executionTimes = executions.get(jobDetails.getJobName());
            executionTimes.add(currentMinute());
            if (executionCount.getAndIncrement() < 3) {
                jobsExecuted[0].countDown();
            } else {
                jobsExecuted[1].countDown();
            }
            subscriber.onNext(null);
            subscriber.onCompleted();
        }));

        setActiveQueue(timeSlice);

        finishedTimeSlice = timeSlice;
        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(jobsExecuted[0].await(10, TimeUnit.SECONDS));
        logger.debug("Waiting for time slice [" + timeSlice.toDate() + "] to finish");
        waitForTimeSliceToFinish(timeSlice, 20, TimeUnit.SECONDS);

        finishedTimeSlice = timeSlice.plusMinutes(1);
        timeSliceFinished = new CountDownLatch(1);
        tickScheduler.advanceTimeTo(timeSlice.plusMinutes(1).getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(jobsExecuted[1].await(10, TimeUnit.SECONDS));
        logger.debug("Waiting for time slice [" + timeSlice.plusMinutes(1).toDate() + "] to finish");
        waitForTimeSliceToFinish(timeSlice.plusMinutes(1), 20, TimeUnit.SECONDS);

        assertEquals(getActiveQueue(), timeSlice.plusMinutes(2));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    private void waitForTimeSliceToFinish(DateTime timeSlice, long timeout, TimeUnit timeUnit) throws Exception {
        finishedTimeSlice = timeSlice;
        assertTrue(timeSliceFinished.await(timeout, timeUnit), "Timed out while waiting for time slice [" +
                timeSlice.toLocalDateTime() + "] to finish. Last finished time slice is [" + finishedTimeSlice + "]");
    }

    private void scheduleJob(JobDetails job) {
        Date timeSlice = new Date(job.getTrigger().getTriggerTime());
        logger.debug("Scheduling " + job + " for execution at " + timeSlice);
        session.execute(insertJob.bind(job.getJobId(), job.getJobType(), job.getJobName(),
                job.getParameters(), SchedulerImpl.getTriggerValue(rxSession, job.getTrigger())));
        session.execute(updateJobQueue.bind(timeSlice, job.getJobId()));
    }

}
