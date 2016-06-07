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

package org.hawkular.metrics.api.jaxrs.influx;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

/**
 * @author Thomas Segismont
 * @deprecated as of 0.17
 */
@Deprecated
public enum InfluxTimeUnit {
    MICROSECONDS("u") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.MICROSECONDS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.MICROSECONDS, sourceValue);
        }
    },
    MILLISECONDS("ms") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.MILLISECONDS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.MILLISECONDS, sourceValue);
        }
    },
    SECONDS("s") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.SECONDS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.SECONDS, sourceValue);
        }
    },
    MINUTES("m") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.MINUTES);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.MINUTES, sourceValue);
        }
    },
    HOURS("h") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.HOURS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.HOURS, sourceValue);
        }
    },
    DAYS("d") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.DAYS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.DAYS, sourceValue);
        }
    },
    WEEKS("w") {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(7 * value, TimeUnit.DAYS);
        }

        @Override
        public long convert(long sourceValue, InfluxTimeUnit sourceUnit) {
            return sourceUnit.convertTo(TimeUnit.DAYS, sourceValue) / 7;
        }
    };

    private String id;

    InfluxTimeUnit(String id) {
        this.id = id.toLowerCase();
    }

    public String getId() {
        return id;
    }

    /**
     * Converts the given <code>value</code> given in <code>this</code> {@link InfluxTimeUnit} to the
     * <code>target</code> {@link java.util.concurrent.TimeUnit}.
     *
     * @param targetUnit the target {@link java.util.concurrent.TimeUnit}
     * @param value      the value in <code>this</code> {@link InfluxTimeUnit}
     *
     * @return the value in the target {@link java.util.concurrent.TimeUnit}
     */
    public abstract long convertTo(TimeUnit targetUnit, long value);

    public abstract long convert(long sourceValue, InfluxTimeUnit sourceUnit);

    private static final Map<String, InfluxTimeUnit> UNIT_BY_ID;

    static {
        ImmutableMap.Builder<String, InfluxTimeUnit> builder = new ImmutableMap.Builder<>();
        for (InfluxTimeUnit influxTimeUnit : values()) {
            builder.put(influxTimeUnit.id, influxTimeUnit);
        }
        UNIT_BY_ID = builder.build();
    }

    /**
     * @param id time unit id
     *
     * @return the {@link InfluxTimeUnit} which id is <code>id</code>, null otherwise
     */
    public static InfluxTimeUnit findById(String id) {
        return UNIT_BY_ID.get(id);
    }
}
