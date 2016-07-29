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

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.scheduler.api.JobDetails;
import org.hawkular.metrics.scheduler.api.Scheduler;
import org.hawkular.metrics.scheduler.api.Trigger;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.joda.time.DateTime;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
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

    private PreparedStatement insertJob;

    private PreparedStatement updateJobQueue;

    TestScheduler() {
    }

    public TestScheduler(RxSession  session) {
        this.session = session;

        finishedTimeSlices = PublishSubject.create();
        jobFinished = PublishSubject.create();

        finishedTimeSlicesSubscriptions = new ArrayList<>();
        jobFinishedSubscriptions = new ArrayList<>();

        insertJob = session.getSession().prepare(
                "INSERT INTO jobs (id, type, name, params, trigger) VALUES (?, ?, ?, ?, ?)");
        updateJobQueue = session.getSession().prepare(
                "INSERT INTO scheduled_jobs_idx (time_slice, job_id) VALUES (?, ?)");
    }

    @Override
    public Single<JobDetails> scheduleJob(String type, String name, Map<String, String> parameters, Trigger trigger) {
        return scheduler.scheduleJob(type, name, parameters, trigger);
    }

    @Override
    public void registerJobFactory(String jobType, Func1<JobDetails, Completable> factory) {
        scheduler.registerJobFactory(jobType, factory);
    }

    @Override
    public void start() {
        try {
            CountDownLatch truncationFinished = new CountDownLatch(1);
            Observable<ResultSet> o1 = session.execute("TRUNCATE jobs");
            Observable<ResultSet> o2 = session.execute("TRUNCATE scheduled_jobs_idx");
            Observable<ResultSet> o3 = session.execute("TRUNCATE finished_jobs_idx");
            Observable<ResultSet> o4 = session.execute("TRUNCATE active_time_slices");
            Observable<ResultSet> o5 = session.execute("TRUNCATE locks");

            Observable.merge(o1, o2, o3, o4, o5).subscribe(
                    resultSet -> {},
                    t -> fail("Truncating tables failed", t),
                    truncationFinished::countDown
            );

            finishedTimeSlicesSubscriptions.forEach(Subscription::unsubscribe);
            jobFinishedSubscriptions.forEach(Subscription::unsubscribe);

            truncationFinished.await();

            DateTimeService.now = DateTime::now;
            tickScheduler = Schedulers.test();
            tickScheduler.advanceTimeTo(currentMinute().getMillis(), TimeUnit.MILLISECONDS);

            DateTimeService.now = () -> new DateTime(tickScheduler.now());

            scheduler = new SchedulerImpl(session);
            scheduler.setTickScheduler(tickScheduler);
            scheduler.setTimeSlicesSubject(finishedTimeSlices);
            scheduler.setJobFinishedSubject(jobFinished);
            scheduler.start();
            advanceTimeBy(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
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
