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
package org.hawkular.metrics.scheduler.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
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
import com.google.common.collect.ImmutableSet;

import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
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

    private PublishSubject<JobDetails> jobFinished;

    private List<Subscription> finishedTimeSlicesSubscriptions;

    private List<Subscription> jobFinishedSubscriptions;

    private PreparedStatement insertJob;
    private PreparedStatement updateJobQueue;

    @BeforeClass
    public void initClass() {
        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(currentMinute().getMillis(), TimeUnit.MILLISECONDS);

        DateTimeService.now = () -> new DateTime(tickScheduler.now());

        finishedTimeSlices = PublishSubject.create();
        jobFinished = PublishSubject.create();

        finishedTimeSlicesSubscriptions = new ArrayList<>();
        jobFinishedSubscriptions = new ArrayList<>();

        insertJob = session.prepare("INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        updateJobQueue = session.prepare("INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");
    }

    private void initJobScheduler() {
        jobScheduler = new SchedulerImpl(rxSession);
        jobScheduler.setConfigurationService(configurationService);
        jobScheduler.setTickScheduler(tickScheduler);
        jobScheduler.setTimeSlicesSubject(finishedTimeSlices);
        jobScheduler.setJobFinishedSubject(jobFinished);
    }

    @BeforeMethod(alwaysRun = true)
    public void initTest(Method method) throws Exception {
        logger.debug("Starting [" + method.getName() + "]");

        CountDownLatch truncationFinished = new CountDownLatch(1);
        Observable<ResultSet> o1 = rxSession.execute("TRUNCATE jobs");
        Observable<ResultSet> o2 = rxSession.execute("TRUNCATE scheduled_jobs_idx");
        Observable<ResultSet> o3 = rxSession.execute("TRUNCATE finished_jobs_idx");
        Observable<ResultSet> o4 = rxSession.execute("TRUNCATE active_time_slices");

        Observable.merge(o1, o2, o3, o4).subscribe(
                resultSet -> {},
                t -> fail("Truncating tables failed", t),
                truncationFinished::countDown
        );

        finishedTimeSlicesSubscriptions.forEach(Subscription::unsubscribe);
        jobFinishedSubscriptions.forEach(Subscription::unsubscribe);

        truncationFinished.await();

        initJobScheduler();
        jobScheduler.start();
    }

    @AfterMethod(alwaysRun = true)
    public void resetJobScheduler() {
        Schedulers.reset();
//        jobScheduler.reset(tickScheduler);
        jobScheduler.shutdown();
    }

    private void restartScheduler(Scheduler scheduler) {
        SchedulerLifecycle lifecycle = (SchedulerLifecycle) scheduler;
        lifecycle.shutdown();
        lifecycle.start();
    }

    private void onTimeSliceFinished(Action1<DateTime> callback) {
        finishedTimeSlicesSubscriptions.add(finishedTimeSlices.subscribe(timeSlice ->
                callback.call(new DateTime(timeSlice))));
    }

    private void onJobFinished(Action1<JobDetails> callback) {
        jobFinishedSubscriptions.add(jobFinished.subscribe(callback::call));
    }

    /**
     * This test runs the scheduler when no jobs are scheduled.
     */
    @Test
    public void advanceClockWhenNoJobsAreScheduled() throws Exception {
        DateTime timeSlice = new DateTime(tickScheduler.now()).plusMinutes(1);

        logger.debug("Setting active time slices to " + timeSlice.toDate());

        CountDownLatch timeSliceFinished = new CountDownLatch(1);
        onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(timeSlice)) {
                timeSliceFinished.countDown();
            }
        });

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(timeSliceFinished.await(10, TimeUnit.SECONDS));

        assertEquals(getActiveTimeSlices(), emptySet());
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
        DateTime timeSlice = new DateTime(tickScheduler.now());

        Trigger trigger = new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build();
        JobDetails jobDetails = new JobDetails(randomUUID(), "Test Type", "Test Job 1", emptyMap(), trigger);
        AtomicInteger executionCountRef = new AtomicInteger();

        jobScheduler.registerJobFactory(jobDetails.getJobType(), details -> Completable.fromAction(() -> {
            logger.debug("Executing " + details);
            executionCountRef.incrementAndGet();
        }));

        scheduleJob(jobDetails);

        CountDownLatch timeSliceFinished = new CountDownLatch(1);
        onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(timeSlice)) {
                timeSliceFinished.countDown();
            }
        });

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);

        assertTrue(timeSliceFinished.await(10, TimeUnit.SECONDS));

        assertEquals(executionCountRef.get(), 1, jobDetails + " should have been executed once");

        assertEquals(getActiveTimeSlices(), emptySet());
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
        DateTime timeSlice = new DateTime(tickScheduler.now());

        Trigger trigger = new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build();
        String jobType = "Test Type";
        Map<String, Integer> executionCounts = new HashMap<>();
        List<JobDetails> jobDetailsList = new ArrayList<>();

        logger.debug("Scheduling jobs for time slice [" + timeSlice.toLocalDateTime() + "]");

        for (int i = 0; i < 3; ++i) {
            JobDetails details = new JobDetails(randomUUID(), jobType, "Test Job " + i, emptyMap(), trigger);
            jobDetailsList.add(details);
            executionCounts.put(details.getJobName(), 0);

            session.execute(insertJob.bind(details.getJobId(), details.getJobType(), details.getJobName(),
                    details.getParameters(), SchedulerImpl.getTriggerValue(rxSession, trigger)));
            session.execute(updateJobQueue.bind(timeSlice.toDate(), details.getJobId()));
        }

        jobScheduler.registerJobFactory(jobType, details -> Completable.fromAction(() -> {
            logger.debug("Executing " + details);
            Integer count = executionCounts.get(details.getJobName());
            executionCounts.put(details.getJobName(), ++count);
            logger.debug("Execution Counts = " + executionCounts);
        }));

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);

        CountDownLatch timeSliceFinished = new CountDownLatch(1);
        onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(timeSlice)) {
                timeSliceFinished.countDown();
            }
        });

        assertTrue(timeSliceFinished.await(10, TimeUnit.SECONDS));
        assertEquals(executionCounts, ImmutableMap.of("Test Job 0", 1, "Test Job 1", 1, "Test Job 2", 1));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
    }

    /**
     * This test executes two single execution jobs. The first job is scheduled earlier than the second, and it does
     * not finish executing until the second job is executed.
     */
    @Test
    public void executeLongRunningSingleExecutionJob() throws Exception {
        final DateTime timeSlice = new DateTime(tickScheduler.now());//.plusMinutes(1);
        logger.debug("TIME is" + timeSlice.toDate());

        JobDetails job1 = new JobDetails(randomUUID(), "Long Test Job", "Long Test Job", emptyMap(),
                new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.getMillis()).build());
        CountDownLatch job1Finished = new CountDownLatch(1);
        CountDownLatch job1Running = new CountDownLatch(1);
        CountDownLatch job2Finished = new CountDownLatch(1);

        jobScheduler.registerJobFactory(job1.getJobType(), details -> Completable.fromAction(() -> {
            try {
                logger.debug("First time slice finished!");
                // This is to let the test know that this job has started executing and the clock can be advanced
                // accordingly.
                job1Running.countDown();

                // Now we wait until the clock is advanced to the later time slice. This allows us to test the scenario
                // in which there is still is a job running that was started in an earlier time slice. The job scheduler
                // has to look for jobs to execute in later time slices.
                job1Finished.await();
            } catch (InterruptedException e) {
                logger.info(details + " was interrupted", e);
            }
        }));

        JobDetails job2 = new JobDetails(randomUUID(), "Test Type", "Test Job", emptyMap(),
                new SingleExecutionTrigger.Builder().withTriggerTime(timeSlice.plusMinutes(1).getMillis()).build());

        jobScheduler.registerJobFactory(job2.getJobType(), details -> Completable.fromAction(() ->
                logger.debug("Executing " + details)));

        scheduleJob(job1);
        scheduleJob(job2);

        CountDownLatch timeSliceFinished = new CountDownLatch(1);
        onTimeSliceFinished(finishedTime -> {
            if (finishedTime.equals(timeSlice.plusMinutes(1))) {
                logger.debug("Finished " + timeSlice.toDate());
                timeSliceFinished.countDown();
            }
        });

        onJobFinished(details -> {
            if (details.equals(job2)) {
                job2Finished.countDown();
            }
        });

        tickScheduler.advanceTimeTo(job1.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS);
        assertTrue(job1Running.await(10, TimeUnit.SECONDS));

        tickScheduler.advanceTimeTo(job2.getTrigger().getTriggerTime(), TimeUnit.MILLISECONDS);

        assertTrue(job2Finished.await(10, TimeUnit.SECONDS));
        job1Finished.countDown();
        assertTrue(timeSliceFinished.await(10, TimeUnit.SECONDS));

//        Thread.sleep(3000);

        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    /**
     * This test schedules and executes a job that repeats every minute. The test runs over a two minute interval.
     */
    @Test
    public void executeJobThatRepeatsEveryMinute() throws Exception {
        Trigger trigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(1, TimeUnit.MINUTES)
                .build();
        DateTime timeSlice = new DateTime(trigger.getTriggerTime());
        JobDetails jobDetails = new JobDetails(randomUUID(), "Repeat Test", "Repeat Test", emptyMap(), trigger);
        TestJob job = new TestJob();

        jobScheduler.registerJobFactory(jobDetails.getJobType(), details -> Completable.fromAction(() -> {
            job.call(details);
        }));

        scheduleJob(jobDetails);

        CountDownLatch firstTimeSliceFinished = new CountDownLatch(1);
        CountDownLatch secondTimeSliceFinished = new CountDownLatch(1);

        onTimeSliceFinished(finishedTimeSlice -> {
            if (timeSlice.equals(finishedTimeSlice)) {
                firstTimeSliceFinished.countDown();
            }
        });
        onTimeSliceFinished(finishedTimeSlice -> {
            if (timeSlice.plusMinutes(1).equals(finishedTimeSlice)) {
                secondTimeSliceFinished.countDown();
            }
        });

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(firstTimeSliceFinished.await(10, TimeUnit.SECONDS));

        tickScheduler.advanceTimeBy(1, TimeUnit.MINUTES);

        assertTrue(secondTimeSliceFinished.await(10, TimeUnit.SECONDS));
        assertEquals(job.getExecutionTimes(), asList(timeSlice, timeSlice.plusMinutes(1)));
        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    /**
     * This test executes multiple repeating jobs multiple times.
     */
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

        for (int i = 0; i < 3; ++i) {
            JobDetails details = new JobDetails(randomUUID(), jobType, jobNamePrefix + i, emptyMap(), trigger);
            executions.put(details.getJobName(), new ArrayList<>());
            scheduleJob(details);
        }

        jobScheduler.registerJobFactory(jobType, details -> Completable.fromAction(() -> {
            logger.debug("Executing " + details);
            List<DateTime> executionTimes = executions.get(details.getJobName());
            executionTimes.add(currentMinute());
        }));

        CountDownLatch firstTimeSliceFinished = new CountDownLatch(1);
        CountDownLatch secondTimeSliceFinished = new CountDownLatch(1);

        onTimeSliceFinished(finishedTimeSlice -> {
            if (timeSlice.equals(finishedTimeSlice)) {
                firstTimeSliceFinished.countDown();
            }
        });
        onTimeSliceFinished(finishedTimeSlice -> {
            if (timeSlice.plusMinutes(1).equals(finishedTimeSlice)) {
                secondTimeSliceFinished.countDown();
            }
        });

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        logger.debug("Waiting for time slice [" + timeSlice.toDate() + "] to finish");
        assertTrue(firstTimeSliceFinished.await(10, TimeUnit.SECONDS));

        tickScheduler.advanceTimeTo(timeSlice.plusMinutes(1).getMillis(), TimeUnit.MILLISECONDS);
        logger.debug("Waiting for time slice [" + timeSlice.plusMinutes(1).toDate() + "] to finish");
        assertTrue(secondTimeSliceFinished.await(10, TimeUnit.SECONDS));

        List<DateTime> expectedTimes = asList(timeSlice, timeSlice.plusMinutes(1));
        executions.entrySet().forEach(entry -> assertEquals(entry.getValue(), expectedTimes));

        assertEquals(getScheduledJobs(timeSlice), emptySet());
        assertEquals(getScheduledJobs(timeSlice.plusMinutes(1)), emptySet());
        assertEquals(getFinishedJobs(timeSlice), emptySet());
        assertEquals(getFinishedJobs(timeSlice.plusMinutes(1)), emptySet());
    }

    /**
     * This test executes a couple different repeating jobs that start at the same time. The first job executes every
     * five minutes while the second job executes every minute. The first job takes the whole five minutes to finish.
     */
    @Test
    public void executeRepeatingJobsWithDifferentIntervals() throws Exception {
        UUID longJobId = randomUUID();
        UUID shortJobId = randomUUID();
        Trigger longTrigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(5, TimeUnit.MINUTES)
                .build();
        Trigger shortTrigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(1, TimeUnit.MINUTES)
                .build();
        DateTime timeSlice = new DateTime(longTrigger.getTriggerTime());
        JobDetails longJob = new JobDetails(longJobId, "Long Job", "Test Job", emptyMap(), longTrigger);
        JobDetails shortJob = new JobDetails(shortJobId, "Short Job", "Test Job", emptyMap(), shortTrigger);

        scheduleJob(longJob);
        scheduleJob(shortJob);

        CountDownLatch timeSliceDone = new CountDownLatch(1);
        onTimeSliceFinished(finishedTimeSlice -> {
            if (finishedTimeSlice.equals(timeSlice)) {
                timeSliceDone.countDown();
            }
        });

        CountDownLatch longJobExecution = new CountDownLatch(5);
        AtomicInteger longJobExecutionCount = new AtomicInteger();
        List<Date> shortJobExecutionTimes = new CopyOnWriteArrayList<>();

        Queue<TestLatch> shortJobExecutions = new LinkedList<>();
        for (int i = 0; i < 5; ++i) {
            shortJobExecutions.offer(new TestLatch(1, timeSlice.plusMinutes(i).toDate()));
        }

        jobScheduler.registerJobFactory(longJob.getJobType(), details -> Completable.fromAction(() -> {
            try {
                longJobExecutionCount.incrementAndGet();
                logger.debug("LONG wait...");
                longJobExecution.await();

                logger.debug("LONG job done!");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        jobScheduler.registerJobFactory(shortJob.getJobType(), details -> Completable.fromAction(() -> {
            try {
                shortJobExecutionTimes.add(new Date(details.getTrigger().getTriggerTime()));
                logger.debug("Executing " + details + " " + shortJobExecutionTimes.size() + " times");
            } catch(Exception e) {
                logger.warn("Failed to execute " + shortJob);
                throw new RuntimeException(e);
            } finally {
                logger.debug("[SHORT] finished!");
            }
        }));

        onJobFinished(details -> {
            if (details.getJobType().equals(shortJob.getJobType())) {
                CountDownLatch latch = shortJobExecutions.peek();
                if (latch != null) {
                    logger.debug("Counting down " + latch);
                    latch.countDown();
                } else {
                    logger.warn("Latch is null");
                }
            }
        });

        DateTime finishedTimeSlice = new DateTime(longTrigger.getTriggerTime());

        for (int i = 0; i < 5; ++i) {
            tickScheduler.advanceTimeTo(finishedTimeSlice.plusMinutes(i).getMillis(), TimeUnit.MILLISECONDS);
            logger.debug("WAIT");
            logger.debug("Waiting on " + shortJobExecutions.peek());
            assertTrue(shortJobExecutions.peek().await(10, TimeUnit.SECONDS), "Remaining short job executions - " +
                    shortJobExecutions + ". Current time is " + new Date(tickScheduler.now()) + ". Execution " +
                    "count is " + shortJobExecutionTimes.size());
            logger.debug("short job finished");
            shortJobExecutions.poll();
            longJobExecution.countDown();
        }

        assertTrue(timeSliceDone.await(10, TimeUnit.SECONDS), "Execpted time slice [" + timeSlice.toDate() +
                "] to have completed");


        DateTime nextTimeSlice = new DateTime(longTrigger.nextTrigger().getTriggerTime());

        assertEquals(longJobExecutionCount.get(), 1, "Expected " + longJob + " to be executed once");
        assertEquals(shortJobExecutionTimes.size(), 5, "Expected " + shortJob + " to be executed 5 times");
        assertEquals(shortJobExecutionTimes, asList(timeSlice.toDate(), timeSlice.plusMinutes(1).toDate(),
                timeSlice.plusMinutes(2).toDate(), timeSlice.plusMinutes(3).toDate(),
                timeSlice.plusMinutes(4).toDate()));
        assertEquals(getScheduledJobs(finishedTimeSlice), emptySet());
        assertEquals(getFinishedJobs(finishedTimeSlice), emptySet());
        assertEquals(getScheduledJobs(nextTimeSlice), ImmutableSet.of(longJob.getJobId(), shortJob.getJobId()));
    }

    @Test
    public void executeLotsOfJobs() throws Exception {
        Trigger trigger = new RepeatingTrigger.Builder()
                .withDelay(1, TimeUnit.MINUTES)
                .withInterval(1, TimeUnit.MINUTES)
                .build();
        DateTime timeSlice = new DateTime(trigger.getTriggerTime());
        String jobType = "Lots of Jobs";
        int numJobs = Runtime.getRuntime().availableProcessors() * 2;
//        int numJobs = 5;
        Random random = new Random();

        logger.debug("Creating and scheduling " + numJobs + " jobs");

        for (int i = 0; i < numJobs; ++i) {
            JobDetails details = new JobDetails(randomUUID(), jobType, "job-" + i, emptyMap(), trigger);
            scheduleJob(details);
        }

        AtomicInteger firstIterationExecutions = new AtomicInteger();
        AtomicInteger secondIterationExecutions = new AtomicInteger();
        CountDownLatch firstIterationJobs = new CountDownLatch(numJobs);
        CountDownLatch secondIterationJobs = new CountDownLatch(numJobs);
        CountDownLatch firstTimeSliceFinished = new CountDownLatch(1);
        CountDownLatch secondTimeSliceFinished = new CountDownLatch(1);
        CountDownLatch thirdTimeSliceFinished = new CountDownLatch(1);

        jobScheduler.registerJobFactory(jobType, details -> Completable.fromAction(() -> {
            logger.debug("Executing " + details);
            long timeout = Math.abs(random.nextLong() % 100);
            DateTime time = new DateTime(details.getTrigger().getTriggerTime());
            logger.debug("Sleeping for " + timeout + " ms");
            try {
                Thread.sleep(timeout);
                if (time.equals(timeSlice)) {
                    firstIterationExecutions.incrementAndGet();
                } else if (time.equals(timeSlice.plusMinutes(1))) {
                    secondIterationExecutions.incrementAndGet();
                }
            } catch (InterruptedException e) {
                logger.info(details + " was interrupted");
            }
        }));

        onJobFinished(details -> {
            logger.debug("Finished executing " + details);
            DateTime time = new DateTime(details.getTrigger().getTriggerTime());
            if (time.equals(timeSlice)) {
                firstIterationJobs.countDown();
                logger.debug("First iteration count is " + firstIterationJobs.getCount());
            } else if (time.equals(timeSlice.plusMinutes(1))) {
                secondIterationJobs.countDown();
                logger.debug("Second iteration count is " + secondIterationJobs.getCount());
            }
        });

        onTimeSliceFinished(finishedTimeSlice -> {
            logger.debug("Finished all work for " + finishedTimeSlice.toDate());
            if (finishedTimeSlice.equals(timeSlice)) {
                logger.debug("First time slice finished");
                firstTimeSliceFinished.countDown();
            } else if (finishedTimeSlice.equals(timeSlice.plusMinutes(1))) {
                logger.debug("Second time slice finished");
                secondTimeSliceFinished.countDown();
            } else if (finishedTimeSlice.equals(timeSlice.plusMinutes(2))) {
                logger.debug("Third time slice finished");
                thirdTimeSliceFinished.countDown();
            } else {
                logger.warn("Did not expect job scheduler to run for time slice [" + finishedTimeSlice.toDate() +
                        "]");
            }
        });

        tickScheduler.advanceTimeTo(timeSlice.getMillis(), TimeUnit.MILLISECONDS);
        Thread.sleep(50);
        tickScheduler.advanceTimeTo(timeSlice.plusMinutes(1).getMillis(), TimeUnit.MILLISECONDS);

        assertTrue(firstIterationJobs.await(30, TimeUnit.SECONDS), "There are " + firstIterationJobs.getCount() +
                " job executions remaining");
        assertTrue(firstTimeSliceFinished.await(30, TimeUnit.SECONDS));
        assertEquals(firstIterationExecutions.get(), numJobs);

        tickScheduler.advanceTimeTo(timeSlice.plusMinutes(2).getMillis(), TimeUnit.MILLISECONDS);

        assertTrue(secondTimeSliceFinished.await(30, TimeUnit.SECONDS));
        assertTrue(thirdTimeSliceFinished.await(30, TimeUnit.SECONDS));

        tickScheduler.advanceTimeTo(timeSlice.plusMinutes(3).getMillis(), TimeUnit.MILLISECONDS);
        assertTrue(secondIterationJobs.await(60, TimeUnit.SECONDS), "There are " + secondIterationJobs.getCount() +
                " job executions remaining");
        assertEquals(secondIterationExecutions.get(), numJobs);
    }

    private class TestLatch extends CountDownLatch {

        private Date timeSlice;

        public TestLatch(int count, Date timeSlice) {
            super(count);
            this.timeSlice = timeSlice;
        }

        @Override public String toString() {
            return "TestLatch{timeSlice=" + timeSlice + "}";
        }
    }

    private void scheduleJob(JobDetails job) {
        Date timeSlice = new Date(job.getTrigger().getTriggerTime());
        logger.debug("Scheduling " + job + " for execution at " + timeSlice);
        session.execute(insertJob.bind(job.getJobId(), job.getJobType(), job.getJobName(),
                job.getParameters(), SchedulerImpl.getTriggerValue(rxSession, job.getTrigger())));
        session.execute(updateJobQueue.bind(timeSlice, job.getJobId()));
    }

    private class TestJob implements Action1<JobDetails> {

        private List<DateTime> executionTimes = new ArrayList<>();

        @Override
        public void call(JobDetails details) {
            logger.debug("Executing " + details);
            executionTimes.add(new DateTime(details.getTrigger().getTriggerTime()));
        }

        public List<DateTime> getExecutionTimes() {
            return executionTimes;
        }
     }

}
