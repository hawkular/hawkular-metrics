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
package org.hawkular.metrics.core.api;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;

import com.google.common.collect.ImmutableMap;

/**
 * An enumeration of the supported metric types.
 *
 * @author John Sanda
 */
public enum MetricType {
    GAUGE(0, "gauge"),
    AVAILABILITY(1, "availability"),
    COUNTER(2, "counter"),
    COUNTER_RATE(3, "counter_rate");

    private int code;
    private String text;

    MetricType(int code, String text) {
        this.code = code;
        this.text = text;
    }

    public int getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return getText();
    }

    private static final ImmutableMap<Integer, MetricType> codes;

    static {
        ImmutableMap.Builder<Integer, MetricType> builder = ImmutableMap.builder();
        Arrays.stream(values()).forEach(type -> builder.put(type.code, type));
        codes = builder.build();
        checkArgument(codes.size() == values().length, "Some metric types have the same code");
    }

    private static final ImmutableMap<String, MetricType> texts;

    static {
        ImmutableMap.Builder<String, MetricType> builder = ImmutableMap.builder();
        Arrays.stream(values()).forEach(type -> builder.put(type.text, type));
        texts = builder.build();
        checkArgument(texts.size() == values().length, "Some metric types have the same string value");
    }

    public static MetricType fromCode(int code) {
        MetricType type = codes.get(code);
        if (type == null) {
            throw new IllegalArgumentException(code + " is not a recognized metric type");
        }
        return type;
    }

    public static MetricType fromTextCode(String textCode) {
        checkArgument(textCode != null, "textCode is null");
        MetricType type = texts.get(textCode);
        if (type == null) {
            throw new IllegalArgumentException(textCode + " is not a recognized metric type");
        }
        return type;
    }

}
