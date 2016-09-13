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
package org.hawkular.metrics.core.service;

import static java.util.Collections.singletonList;

import static org.hawkular.metrics.core.service.UUIDGen.getTimeUUID;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.joda.time.DateTime.now;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hawkular.metrics.core.jobs.JobsServiceImpl;
import org.hawkular.metrics.core.service.metrics.BaseMetricsITest;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.jboss.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.Minutes;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class WriteTest extends BaseMetricsITest {

    private static Logger logger = Logger.getLogger(WriteTest.class);

    MetricRegistry registry;
    Meter inserts;
    Timer insertTime;
    ConsoleReporter consoleReporter;
    AtomicLong writes;
    AtomicInteger failures;
    Random random;
    ScheduledExecutorService writers;
    ScheduledExecutorService cacheManager;
    int numWriters;

    JobsServiceImpl jobsService;

//    @BeforeClass
//    public void init() throws Exception {
//        ConfigurationService configurationService = new ConfigurationService();
//        configurationService.init(rxSession);
//
//        RollupServiceImpl rollupService = new RollupServiceImpl(rxSession);
//        rollupService.init();
//
//        SchedulerImpl scheduler = new SchedulerImpl(rxSession);
//
//        jobsService = new JobsServiceImpl();
//        jobsService.setSession(rxSession);
//        jobsService.setCacheService(cacheService);
//        jobsService.setConfigurationService(configurationService);
//        jobsService.setRollupService(rollupService);
//        jobsService.setMetricsService(metricsService);
//        jobsService.setScheduler(scheduler);
//
//        jobsService.start();
//
//        registry = new MetricRegistry();
//        inserts = registry.meter("inserts");
//        insertTime = registry.timer("insert-time");
//
//        File logDir = new File("target");
//        consoleReporter = ConsoleReporter.forRegistry(registry)
//                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
//                .outputTo(new PrintStream(new FileOutputStream(new File(logDir, "metrics.txt")))).build();
//        consoleReporter.start(20, TimeUnit.SECONDS);
//        numWriters = 8;
//        writers = Executors.newScheduledThreadPool(numWriters, new ThreadFactoryBuilder()
//                .setNameFormat("writer-pool-%d").build());
//        cacheManager = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
//                .setNameFormat("cache-manager-pool-%d").build());
//        random = new Random();
//        writes = new AtomicLong();
//        failures = new AtomicInteger();
//    }

    @AfterClass
    public void shutdown() throws Exception {
        writers.shutdown();
        writers.awaitTermination(5, TimeUnit.SECONDS);
        cacheManager.shutdown();
        cacheManager.awaitTermination(1, TimeUnit.SECONDS);
        consoleReporter.stop();

        logger.info("Total Writes: " + writes + ", Total Failures: " + failures);
    }


//    @Test
    public void pumpWrites() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        Meter inserts = registry.meter("inserts");

        File logDir = new File("target");
        ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(registry)
                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
                .outputTo(new PrintStream(new FileOutputStream(new File(logDir, "metrics.txt")))).build();
        consoleReporter.start(20, TimeUnit.SECONDS);

        PreparedStatement insert = session.prepare(
                "UPDATE data " +
                        "USING TTL ? " +
                        "SET n_value = ? " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");
        AtomicLong writes = new AtomicLong();
        AtomicInteger failures = new AtomicInteger();
        Random random = new Random();
        RateLimiter permits = RateLimiter.create(300000);

        DateTime end = now().plusMinutes(20);
        AtomicLong time = new AtomicLong(now().minusHours(10).getMillis());
        String tenant = "PumpWrites";
        String metric = "G1";
        int ttl = 300;

        while (now().isBefore(end)) {
            permits.acquire(1);
            ResultSetFuture future = session.executeAsync(insert.bind(ttl, random.nextDouble(), tenant, (byte) 0,
                    metric, 0L, getTimeUUID(time.getAndAdd(10))));
            Futures.addCallback(future, new FutureCallback<ResultSet>() {
                @Override
                public void onSuccess(ResultSet result) {
                    writes.incrementAndGet();
                    inserts.mark();
                }

                @Override
                public void onFailure(Throwable t) {
                    failures.incrementAndGet();
                    logger.warn("Write failed", t);
                    logger.info("Failure Count: " + failures);
                }
            });
        }
        logger.info("Total Writes: " + writes + ", Total Failures: " + failures);

        consoleReporter.stop();
    }

    @Test
    public void writeWithoutCaching() throws Exception {
        Runnable purgeCache = () -> {
            try {
                long time = DateTimeService.currentMinute().minusMinutes(1).getMillis();
                logger.info("Removing cache entries for " + new Date(time));
//                cacheService.getRawDataCache().getAdvancedCache().removeGroup(Long.toString(time));
            } catch (Exception e) {
                logger.warn("Failed to purge cache entries", e);
            }
        };
//        cacheManager.scheduleAtFixedRate(purgeCache, 1, 1, TimeUnit.MINUTES);

        for (int i = 0; i < numWriters; ++i) {
            writers.scheduleAtFixedRate(createWriter(i), 0L, 1, TimeUnit.SECONDS);
        }
        Thread.sleep(Minutes.minutes(10).toStandardDuration().getMillis());
//        DateTime end = now().plusMinutes(6);
//        AtomicLong time = new AtomicLong(now().minusHours(10).getMillis());
//        String tenant = "PumpWrites";
//        int numMetrics = 1000;
//
//        while (now().isBefore(end)) {
//            List<Metric<Double>> metrics = new ArrayList<>();
//            for (int i = 0; i < numMetrics; ++i) {
//                MetricId<Double> metricId = new MetricId<>(tenant, GAUGE, "G" + i);
//                metrics.add(new Metric<>(metricId, singletonList(new DataPoint<>(time.get(), random.nextDouble()))));
//            }
//            time.addAndGet(20);
//            Completable updates = metricsService.addDataPoints(GAUGE, Observable.from(metrics)).toCompletable();
//            updates.subscribe(
//                    () -> {
//                        inserts.mark(numMetrics);
//                        writes.addAndGet(numMetrics);
//                    },
//                    t -> {
//                        logger.warn("Inserts failed", t);
//                        failures.getAndIncrement();
//                    }
//            );
//        }
//
//        logger.info("Total Writes: " + writes + ", Total Failures: " + failures);
//        consoleReporter.stop();
    }

    private Runnable createWriter(int id) {
        return () -> {
            try {
                Stopwatch stopwatch = Stopwatch.createStarted();
                String tenant = "T" + id;
                int numMetrics = 500;
                List<Metric<Double>> metrics = new ArrayList<>();
                for (int i = 0; i < numMetrics; ++i) {
                    MetricId<Double> metricId = new MetricId<>(tenant, GAUGE, "G" + i);
                    metrics.add(new Metric<>(metricId, singletonList(new DataPoint<>(System.currentTimeMillis(),
                            random.nextDouble()))));
                }
                Completable updates = metricsService.addDataPoints(GAUGE, Observable.from(metrics)).toCompletable();
                updates.subscribe(
                        () -> {
                            stopwatch.stop();
                            insertTime.update(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                            inserts.mark(numMetrics);
                            logger.debug("Inserted " + numMetrics + " in " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +
                                    " ms");
                            writes.addAndGet(numMetrics);
                        },
                        t -> {
                            logger.warn("Inserts failed", t);
                            failures.getAndIncrement();
                            logger.info("Failure Count: " + failures);
                        }
                );
//                latch.await();
            } catch (Exception e) {
                logger.warn("Insert failed", e);
            }
        };
    }

}
