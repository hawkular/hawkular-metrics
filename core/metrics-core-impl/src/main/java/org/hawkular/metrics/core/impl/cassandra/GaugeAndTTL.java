/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl.cassandra;

import org.hawkular.metrics.core.api.Gauge;

/**
 * This class has been introduced (temporarily) as part of the RxJava refactoring. When we are persisting data I think
 * that the TTL should be set on the metric. Initially I did that, but it resulted in several test failures. After
 * fixing a couple of the failures I backed off the approach since it looked like I might have changed some of the
 * current behavior for inserting data. Changing behavior is outside the scope of the initial RxJava migration; so, we
 * have this class for now.
 *
 * @author jsanda
 */
public class GaugeAndTTL {

    public Gauge gauge;

    public int ttl;

    public GaugeAndTTL(Gauge gauge, int ttl) {
        this.gauge = gauge;
        this.ttl = ttl;
    }

}
