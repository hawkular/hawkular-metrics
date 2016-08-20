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

import java.util.List;

import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.NumericBucketPoint;

import rx.Completable;
import rx.Observable;

/**
 * @author jsanda
 */
public interface RollupService {

    enum RollupBucket {

        ROLLUP60(60),       // 1 minute

        ROLLUP300(300),     // 5 minutes

        ROLLUP3600(3600);   // 1 hour

        private int durationInSec;

        RollupBucket(int duration) {
            durationInSec = duration;
        }

        public int getDuration() {
            return durationInSec;
        }

    }

    Completable insert(MetricId<Double> id, NumericBucketPoint dataPoint, int rollup);

    Observable<NumericBucketPoint> find(MetricId<Double> id, long start, long end, int rollup);

    Observable<List<Rollup>> getRollups(MetricId<Double> id, int rawTTL);
}
