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
package org.hawkular.metrics.scheduler.impl;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.RetryPolicy;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;

import com.datastax.driver.core.PreparedStatement;

import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * @author jsanda
 */
public class TestScheduler implements Scheduler {

    private RxSession session;

    private SchedulerImpl scheduler;

    /**
     * Used in place of SchedulerImpl.tickScheduler so that we can control the clock.
     */
    private rx.schedulers.TestScheduler tickScheduler;

    /**
     * Publishes notifications when the job scheduler finishes executing jobs for a time slice (or if there are no
     * jobs to execute). This a test hook that allows to verify the state of things incrementally as work is completed.
     */
    private PublishSubject<Date> finishedTimeSlices;

    private PublishSubject<JobDetails> jobFinished;

    private List<Subscription> finishedTimeSlicesSubscriptions;

    private List<Subscription> jobFinishedSubscriptions;

    @SuppressWarnings("unused")
    private PreparedStatement insertJob;

    @SuppressWarnings("unused")
    private PreparedStatement updateJobQueue;

    TestScheduler() {
    }

    public TestScheduler(RxSession session) {
        this.session = session;

        finishedTimeSlices = PublishSubject.create();
        jobFinished = PublishSubject.create();

        finishedTimeSlicesSubscriptions = new ArrayList<>();
        jobFinishedSubscriptions = new ArrayList<>();

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        updateJobQueue = session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");

        initTickScheduler();
        initJobScheduler(null);
    }

    public TestScheduler(RxSession session, JobsService jobsService) {
        this.session = session;

        finishedTimeSlices = PublishSubject.create();
        jobFinished = PublishSubject.create();

        finishedTimeSlicesSubscriptions = new ArrayList<>();
        jobFinishedSubscriptions = new ArrayList<>();

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        updateJobQueue = session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");

        initTickScheduler();
        initJobScheduler(jobsService);
    }

    @Override
    public Single<? extends JobDetails> scheduleJob(String type, String name, Map<String, String> parameters,
            Trigger trigger) {
        return scheduler.scheduleJob(type, name, parameters, trigger);
    }

    @Override
    public void register(String jobType, Func1<JobDetails, Completable> factory) {
        scheduler.register(jobType, factory);
    }

    @Override
    public void register(String jobType, Func1<JobDetails, Completable> jobProducer,
            Func2<JobDetails, Throwable, RetryPolicy> retryFunction) {
        scheduler.register(jobType, jobProducer, retryFunction);
    }

    @Override
    public Completable unscheduleJobById(String jobId) {
        return scheduler.unscheduleJobById(jobId);
    }

    @Override
    public Completable unscheduleJobByTypeAndName(String jobType, String jobName) {
        return scheduler.unscheduleJobByTypeAndName(jobType, jobName);
    }

    @Override
    public void start() {
            scheduler.start();
    }

    public void truncateTables(String keyspace) {
        //TODO: The filtering below for static data tables is prone to error. Find a better way to avoid
        //truncating those tables.
        session.execute("select table_name from system_schema.tables where keyspace_name = '" + keyspace + "'")
                .flatMap(Observable::from)
                .filter(row -> !row.getString(0).equals("cassalog") && !row.getString(0).equals("sys_config"))
                .flatMap(row -> session.execute("truncate " + row.getString(0)))
                .toCompletable()
                .await(10, TimeUnit.SECONDS);
    }

    private void initJobScheduler(JobsService jobService) {
        try {
            if (jobService == null) {
                scheduler = new SchedulerImpl(session, InetAddress.getLocalHost().getHostName());
            } else {
                scheduler = new SchedulerImpl(session, InetAddress.getLocalHost().getHostName(), jobService);
            }
            scheduler.setTickScheduler(tickScheduler);
            scheduler.setTimeSlicesSubject(finishedTimeSlices);
            scheduler.setJobFinishedSubject(jobFinished);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private void initTickScheduler() {
        DateTimeService.now = DateTime::now;
        tickScheduler = Schedulers.test();
        tickScheduler.advanceTimeTo(currentMinute().getMillis(), TimeUnit.MILLISECONDS);

        DateTimeService.now = () -> new DateTime(tickScheduler.now());
    }

    @Override
    public void shutdown() {
        finishedTimeSlicesSubscriptions.forEach(Subscription::unsubscribe);
        jobFinishedSubscriptions.forEach(Subscription::unsubscribe);
        Schedulers.reset();
        scheduler.shutdown();
    }

    public void onTimeSliceFinished(Action1<DateTime> callback) {
        finishedTimeSlicesSubscriptions.add(finishedTimeSlices.subscribe(timeSlice ->
                callback.call(new DateTime(timeSlice))));
    }

    public void onJobFinished(Action1<JobDetails> callback) {
        jobFinishedSubscriptions.add(jobFinished.subscribe(callback::call));
    }

    public long now() {
        return tickScheduler.now();
    }

    public void advanceTimeTo(long timestamp) {
        tickScheduler.advanceTimeTo(timestamp, TimeUnit.MILLISECONDS);
    }

    public void advanceTimeBy(int minutes) {
        tickScheduler.advanceTimeBy(minutes, TimeUnit.MINUTES);
    }
}
