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
package org.hawkular.metrics.api.jaxrs.param;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.ext.ParamConverter;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableBiMap.Builder;

/**
 * A JAX-RS {@link ParamConverter} for {@link Duration} parameters. The list of valid time units is the following:
 * <ul>
 *     <li><em>ms</em>: milliseconds</li>
 *     <li><em>s</em>: seconds</li>
 *     <li><em>mn</em>: minutes</li>
 *     <li><em>h</em>: hours</li>
 *     <li><em>d</em>: days</li>
 * </ul>
 *
 * @author Thomas Segismont
 */
public class DurationConverter implements ParamConverter<Duration> {
    private static final ImmutableBiMap<TimeUnit, String> UNITS = new Builder<TimeUnit, String>()
            .put(MILLISECONDS, "ms")
            .put(SECONDS, "s")
            .put(MINUTES, "mn")
            .put(HOURS, "h")
            .put(DAYS, "d")
            .build();
    private static final Pattern REGEXP = Pattern.compile(
            "(\\d+)"
            + "("
            + UNITS.values().stream().collect(joining("|"))
            + ")"
    );

    @Override
    public Duration fromString(String value) {
        Matcher matcher = REGEXP.matcher(value);
        checkArgument(matcher.matches(), "Invalid duration %s", value);
        return new Duration(Long.valueOf(matcher.group(1)), UNITS.inverse().get(matcher.group(2)));
    }

    @Override
    public String toString(Duration duration) {
        checkArgument(UNITS.containsKey(duration.getTimeUnit()), "Invalid unit %s", duration.getTimeUnit());
        return duration.getValue() + UNITS.get(duration.getTimeUnit());
    }
}
