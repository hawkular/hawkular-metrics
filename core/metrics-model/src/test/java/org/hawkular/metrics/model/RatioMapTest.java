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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.Test;

/**
 * @author Joel Takvorian
 */
public class RatioMapTest {

    @Test
    public void shouldBuildRatioMap() {
        RatioMap map = RatioMap.builder()
                .add("apple")
                .add("apple")
                .add("peer")
                .add("apple")
                .add("orange")
                .add("orange")
                .build();

        assertThat(map).isNotNull();
        assertThat(map.getSamples()).isEqualTo(6);
        assertThat(map.getRatios()).containsOnlyKeys("apple", "peer", "orange");
        assertThat(map.getRatios().get("apple")).isCloseTo(0.5, within(0.0001));
        assertThat(map.getRatios().get("orange")).isCloseTo(0.33333, within(0.0001));
        assertThat(map.getRatios().get("peer")).isCloseTo(0.16667, within(0.0001));
    }
}