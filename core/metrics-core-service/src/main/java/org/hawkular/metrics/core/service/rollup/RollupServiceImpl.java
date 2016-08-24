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
package org.hawkular.metrics.core.service.rollup;

import static java.util.Arrays.asList;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;
import org.hawkular.rx.cassandra.driver.RxSession;

import com.datastax.driver.core.PreparedStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public class RollupServiceImpl implements RollupService {

    private RxSession session;

    private Map<Integer, PreparedStatement> inserts;

    private Map<Integer, PreparedStatement> finders;

    private RateLimiter writePermits;

    public RollupServiceImpl(RxSession session) {
        this.session = session;
    }

    public void init() {
        inserts = ImmutableMap.of(
                60, session.getSession().prepare(getInsertCQL(60)),
                300, session.getSession().prepare(getInsertCQL(300)),
                3600, session.getSession().prepare(getInsertCQL(3600)));
        finders = ImmutableMap.of(
                60, session.getSession().prepare(getFinderCQL(60)),
                300, session.getSession().prepare(getFinderCQL(300)),
                3600, session.getSession().prepare(getFinderCQL(3600)));
        writePermits = RateLimiter.create(50.0);
    }

    private String getInsertCQL(int rollup) {
        return "INSERT INTO rollup" + rollup + " (tenant_id, metric, shard, time, min, max, avg, median, sum, " +
                "samples, percentiles) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private String getFinderCQL(int rollup) {
        return "SELECT time, min, max, avg, median, sum, samples, percentiles FROM rollup" + rollup + " " +
                "WHERE tenant_id = ? AND metric = ? AND shard = 0 AND time >= ? AND time < ?";
    }

    @Override
    public Completable insert(MetricId<Double> id, NumericBucketPoint dataPoint, int rollup) {
        PreparedStatement insert = inserts.get(rollup);
        checkNotNull(insert, "There is no " + rollup + " rollup");
        // TODO implement some form of back pressure
        // Computing rollups introduces substantial overhead in terms of Cassandra requests. Suppose with each request
        // to insert data points we have 1 data per per metric and that we have N metrics. By default as of right now
        // we compute 1 minute, 5 minute, and 1 hour rollups. This can result in doing 2N writes each minute, 3N writes
        // every 5 minutes, and 4N writes every hour. In performance/stress testing I have done this far, this spike
        // in requests can easily result in NoHostAvailableExceptions from the driver. We want ingestion of data to
        // be as fast as it can, but writing rollups which happen in background job do not have to be as fast.
        writePermits.acquire();
        return session.execute(insert.bind(id.getTenantId(), id.getName(), 0L, new Date(dataPoint.getStart()),
                dataPoint.getMin(), dataPoint.getMax(), dataPoint.getAvg(), dataPoint.getMedian(),
                dataPoint.getSum(), dataPoint.getSamples(), toMap(dataPoint.getPercentiles()))).toCompletable();

    }

    @Override
    public Observable<NumericBucketPoint> find(MetricId<Double> id, long start, long end, int rollup) {
        PreparedStatement finder = finders.get(rollup);
        checkNotNull(finder, "There is no " + rollup + " rollup");
        return session.execute(finder.bind(id.getTenantId(), id.getName(), new Date(start), new Date(end)))
                .flatMap(Observable::from)
                .map(row -> new NumericBucketPoint(
                        row.getTimestamp(0).getTime(),
                        row.getTimestamp(0).getTime() + TimeUnit.SECONDS.toMillis(rollup),
                        row.getDouble(1),
                        row.getDouble(3),
                        row.getDouble(4),
                        row.getDouble(2),
                        row.getDouble(5),
                        getPercentiles(row.getMap(7, Float.class, Double.class)),
                        row.getInt(6)));
    }

    private List<Percentile> getPercentiles(Map<Float, Double> map) {
        return map.entrySet().stream().map(entry -> new Percentile(entry.getKey().toString(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private Map<Float, Double> toMap(List<Percentile> percentiles) {
        Map<Float, Double> map = new HashMap<>();
        percentiles.forEach(p -> map.put((float) p.getQuantile(), p.getValue()));
        return map;
    }

    @Override
    public Observable<List<Rollup>> getRollups(MetricId<Double> id, int rawTTL) {
        return Observable.just(asList(
                new Rollup(RollupBucket.ROLLUP60.getDuration(), rawTTL * 2),
                new Rollup(RollupBucket.ROLLUP300.getDuration(), rawTTL * 4),
                new Rollup(RollupBucket.ROLLUP3600.getDuration(), rawTTL * 6)
        ));
    }
}
