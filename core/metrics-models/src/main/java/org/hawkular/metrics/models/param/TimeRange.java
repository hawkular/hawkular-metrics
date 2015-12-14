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
package org.hawkular.metrics.models.param;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hawkular.metrics.models.Utils.isValidTimeRange;

import com.google.common.base.Objects;

/**
 * A JAX-RS parameter object used to build time ranges from query params.
 *
 * @author Thomas Segismont
 */
public class TimeRange {
    static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    private final long start;
    private final long end;
    private final boolean valid;
    private final String problem;

    public TimeRange(Long start, Long end) {
        long now = System.currentTimeMillis();
        this.start = start == null ? now - EIGHT_HOURS : start;
        this.end = end == null ? now : end;
        if (!isValidTimeRange(this.start, this.end)) {
            valid = false;
            problem = "Range end must be strictly greater than start";
        } else {
            valid = true;
            problem = null;
        }
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public boolean isValid() {
        return valid;
    }

    public String getProblem() {
        return problem;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("start", start)
                .add("end", end)
                .add("valid", valid)
                .add("problem", problem)
                .omitNullValues()
                .toString();
    }
}
