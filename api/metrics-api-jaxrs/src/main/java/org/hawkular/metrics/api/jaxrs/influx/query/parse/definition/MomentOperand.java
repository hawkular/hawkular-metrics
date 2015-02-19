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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

import java.util.concurrent.TimeUnit;

import org.joda.time.Instant;

/**
 * @author Thomas Segismont
 */
public class MomentOperand implements InstantOperand {
    private final String functionName;
    private final int timeshift;
    private final InfluxTimeUnit timeshiftUnit;

    public MomentOperand(String functionName, int timeshift, InfluxTimeUnit timeshiftUnit) {
        this.functionName = functionName;
        this.timeshift = timeshift;
        this.timeshiftUnit = timeshiftUnit;
    }

    public String getFunctionName() {
        return functionName;
    }

    public int getTimeshift() {
        return timeshift;
    }

    public InfluxTimeUnit getTimeshiftUnit() {
        return timeshiftUnit;
    }

    @Override
    public Instant getInstant() {
        return Instant.now().plus(timeshiftUnit.convertTo(TimeUnit.MILLISECONDS, timeshift));
    }
}
