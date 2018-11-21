/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.dropwizard;

import static org.hawkular.metrics.dropwizard.MetricsTagger.METRIC_TYPE_COUNTER;
import static org.hawkular.metrics.dropwizard.MetricsTagger.METRIC_TYPE_GAUGE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.codahale.metrics.Counting;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Sampling;

/**
 * @author Joel Takvorian
 */
final class MetricsDecomposer {

    private static final List<MetricPart<Counting, Long>> COUNTINGS;
    private static final List<MetricPart<Metered, Object>> METERED;
    private static final List<MetricPart<Sampling, Object>> SAMPLING;

    private final Map<String, Set<String>> namedMetricsComposition;
    private final Collection<RegexContainer<Set<String>>> regexComposition;

    static {
        COUNTINGS = new ArrayList<>(1);
        COUNTINGS.add(part(Counting::getCount, "count", METRIC_TYPE_COUNTER));
        METERED = new ArrayList<>(4);
        METERED.add(part(Metered::getOneMinuteRate, "1minrt", METRIC_TYPE_GAUGE));
        METERED.add(part(Metered::getFiveMinuteRate, "5minrt", METRIC_TYPE_GAUGE));
        METERED.add(part(Metered::getFifteenMinuteRate, "15minrt", METRIC_TYPE_GAUGE));
        METERED.add(part(Metered::getMeanRate, "meanrt", METRIC_TYPE_GAUGE));
        SAMPLING = new ArrayList<>(10);
        SAMPLING.add(part(s -> s.getSnapshot().getMin(), "min", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().getMax(), "max", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().getMean(), "mean", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().getMedian(), "median", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().getStdDev(), "stddev", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().get75thPercentile(), "75perc", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().get95thPercentile(), "95perc", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().get98thPercentile(), "98perc", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().get99thPercentile(), "99perc", METRIC_TYPE_GAUGE));
        SAMPLING.add(part(s -> s.getSnapshot().get999thPercentile(), "999perc", METRIC_TYPE_GAUGE));
    }

    MetricsDecomposer(Map<String, Set<String>> namedMetricsComposition,
                      Collection<RegexContainer<Set<String>>> regexComposition) {
        this.namedMetricsComposition = namedMetricsComposition;
        this.regexComposition = regexComposition;
    }

    Optional<Collection<String>> getAllowedParts(String metricName) {
        if (namedMetricsComposition.containsKey(metricName)) {
            return Optional.of(namedMetricsComposition.get(metricName));
        } else {
            for (RegexContainer<Set<String>> reg : regexComposition) {
                Optional<Set<String>> match = reg.match(metricName);
                if (match.isPresent()) {
                    return Optional.of(match.get());
                }
            }

        }
        return Optional.empty();
    }

    private static <T,U> MetricPart<T,U> part(Function<T,U> getter, String suffix, String type) {
        return new MetricPart<T, U>() {
            @Override public U getData(T input) {
                return getter.apply(input);
            }

            @Override public String getSuffix() {
                return suffix;
            }

            @Override public String getMetricType() {
                return type;
            }
        };
    }

    PartsStreamer streamParts(String metricName) {
        Predicate<String> p = getAllowedParts(metricName)
                .map(allowed -> (Predicate<String>)(allowed::contains))
                .orElse(part -> true);
        return new PartsStreamer(p);
    }

    static class PartsStreamer {
        private final Predicate<String> metricPredicate;
        private PartsStreamer(Predicate<String> metricPredicate) {
            this.metricPredicate = metricPredicate;
        }

        Stream<MetricPart<Counting, Long>> countings() {
            return COUNTINGS.stream()
                    .filter(metricPart -> metricPredicate.test(metricPart.getSuffix()));
        }

        Stream<MetricPart<Metered, Object>> metered() {
            return METERED.stream()
                    .filter(metricPart -> metricPredicate.test(metricPart.getSuffix()));
        }

        Stream<MetricPart<Sampling, Object>> samplings() {
            return SAMPLING.stream()
                    .filter(metricPart -> metricPredicate.test(metricPart.getSuffix()));
        }
    }
}
