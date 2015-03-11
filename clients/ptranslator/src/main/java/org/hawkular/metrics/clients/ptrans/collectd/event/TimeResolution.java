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
package org.hawkular.metrics.clients.ptrans.collectd.event;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * The precision of a time duration.
 *
 * @author Thomas Segismont
 */
public enum TimeResolution {
    /**
     * second precision. *
     */
    SECONDS,
    /**
     * 2<sup>-30</sup> seconds precision. *
     */
    HIGH_RES;

    private static final double HIGH_RES_PRECISION = Math.pow(2, 30);

    /**
     * Converts to milliseconds the given <code>timeSpan</code>.
     *
     * @param timeSpan time duration to convert
     *
     * @return time duration in milliseconds
     */
    public static long toMillis(TimeSpan timeSpan) {
        return toMillis(timeSpan.getValue(), timeSpan.getResolution());
    }

    /**
     * Converts to milliseconds the duration <code>val</code>, given at the {@link TimeResolution}
     * <code>resolution</code>.
     *
     * @param val        the duration
     * @param resolution the precision
     *
     * @return time duration in milliseconds
     */
    public static long toMillis(long val, TimeResolution resolution) {
        if (resolution == SECONDS) {
            return TimeUnit.MILLISECONDS.convert(val, TimeUnit.SECONDS);
        }
        return (long) ((1000d * (double) val) / HIGH_RES_PRECISION);
    }

    /**
     * Converts to {@link Date} the given <code>timestamp</code>.
     *
     * @param timestamp time duration since epoch
     *
     * @return the {@link Date} corresponding to the given <code>timestamp</code>
     */
    public static Date toDate(TimeSpan timestamp) {
        return toDate(timestamp.getValue(), timestamp.getResolution());
    }

    /**
     * Converts to {@link Date} the <code>timestamp</code>, given at the {@link TimeResolution}
     * <code>resolution</code>.
     *
     * @param timestamp  time duration since epoch
     * @param resolution the <code>timestamp</code> precision
     *
     * @return the {@link Date} corresponding to the given <code>timestamp</code>
     */
    public static Date toDate(long timestamp, TimeResolution resolution) {
        return new Date(toMillis(timestamp, resolution));
    }
}
