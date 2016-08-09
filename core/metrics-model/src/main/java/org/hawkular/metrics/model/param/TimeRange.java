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
package org.hawkular.metrics.model.param;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.hawkular.metrics.model.Utils.isValidTimeRange;

import com.google.common.base.MoreObjects;

/**
 * A JAX-RS parameter object used to build time ranges from query params.
 *
 * @author Thomas Segismont
 */
public class TimeRange {
    static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    private long start;
    private long end;
    private boolean valid;
    private String problem;

    public TimeRange (String start, String end) {
        long now = System.currentTimeMillis();

        try {
            this.start = (start == null || start.isEmpty()) ? now - EIGHT_HOURS : handleOffsets(start);
            this.end = (end == null || end.isEmpty()) ? now : handleOffsets(end);
            if (!isValidTimeRange(this.start, this.end)) {
                valid = false;
                problem = "Range end must be strictly greater than start";
            } else {
                valid = true;
                problem = null;
            }
        } catch (NumberFormatException e) {
            valid = false;
            problem = "The start (" + start + ") or end (" + end + ") value does not correspond to a valid number.";
        } catch (IllegalArgumentException e) {
            valid = false;
            problem = "The start (" + start + ") or end (" + end + ") value does not correspond to a valid duration.";
        }
    }

    private Long handleOffsets(String number) throws NumberFormatException, IllegalArgumentException{
        long now = System.currentTimeMillis();
        if (number.startsWith("+")) {
            String offset = number.substring(1).trim();
            Duration duration = new Duration(offset);
            return now + duration.toMillis();
       } else if (number.startsWith("-")) {
            String offset = number.substring(1).trim();
            Duration duration = new Duration(offset);
            return now - duration.toMillis();
        } else {
            return Long.parseLong(number);
        }
    }

    public TimeRange(Long start, Long end) {
        this(start == null? null: start.toString(), end == null ? null: end.toString());
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
        return MoreObjects.toStringHelper(this)
                .add("start", start)
                .add("end", end)
                .add("valid", valid)
                .add("problem", problem)
                .omitNullValues()
                .toString();
    }
}
