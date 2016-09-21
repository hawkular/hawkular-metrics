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
package org.hawkular.metrics.model;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * Map of ratios for multiple aggregated availability or string metrics.
 *
 * @author Joel Takvorian
 */
public final class RatioMap {
    private final Map<String, Double> ratios;
    private final int samples;

    public RatioMap(Map<String, Double> ratioMap, int samples) {
        this.ratios = ratioMap;
        this.samples = samples;
    }

    public Map<String, Double> getRatios() {
        return ratios;
    }

    public Double getRatio(String key) {
        return ratios.getOrDefault(key, 0d);
    }

    public int getSamples() {
        return samples;
    }

    @Override public String toString() {
        return "RatioMap[" +
                "ratios=" + ratios +
                "samples=" + samples +
                ']';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Multiset<String> counter = HashMultiset.create();
        private int total = 0;

        public Builder add(String item) {
            total++;
            counter.add(item);
            return this;
        }

        public RatioMap build() {
            Map<String, Double> ratio = new HashMap<>();
            for (String item : counter.elementSet()) {
                ratio.put(item, (double)counter.count(item) / (double)total);
            }
            return new RatioMap(ratio, total);
        }
    }
}
