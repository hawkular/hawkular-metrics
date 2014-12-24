/*
 * Copyright 2014 Red Hat, Inc.
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

package org.rhq.metrics.core;

/**
 * A set of pre-computed aggregates over a time interval (called <i>bucket</i> here) or a raw
 * numeric data for a time instant.
 *
 * @author John Sanda
 */
public interface NumericMetric {

    /**
     * Returns the identifier that defines a time interval such as 1 minute, 5 minutes, 1 hour,
     * 1 day, etc. over which the aggregates available in  this {@link NumericMetric} were computed.
     * Another possible names for <i>bucket</i> are <i>roll-up</i> or <i>time window</i>.
     * <p>
     * {@code "raw"} is a special value meaning that this {@link NumericMetric} stores a raw instant
     * data rather than data aggregated over a time window.
     *
     * @return the identifier of the bucket.
     */
    String getBucket();

    String getId();

    Double getMin();

    Double getMax();

    Double getAvg();

    /**
     * For aggregate Metrics, returns the beginning of the bucket, whereas for raw metrics, returns
     * time when the data point was collected
     *
     * @return the timestamp
     */
    long getTimestamp();

}
