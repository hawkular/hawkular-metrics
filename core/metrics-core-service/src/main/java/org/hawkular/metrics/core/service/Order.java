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

package org.hawkular.metrics.core.service;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;

/**
 * @author John Sanda
 */
public enum Order {
    ASC("asc"),
    DESC("desc");

    private static final Map<String, Order> texts;

    static {
        Builder<String, Order> builder = ImmutableSortedMap.orderedBy(String.CASE_INSENSITIVE_ORDER);
        for (Order order : values()) {
            builder.put(order.text, order);
        }
        texts = builder.build();
    }

    private String text;

    Order(String text) {
        this.text = text;
    }

    public static Order fromText(String text) {
        checkArgument(text != null, "text is null");
        Order order = texts.get(text);
        if (order == null) {
            throw new IllegalArgumentException(text + " is not a recognized order");
        }
        return order;
    }

    /**
     * Determines a default value based on API parameters.
     *
     * @param limit maximum of rows returned
     * @param start time range start
     * @param end   time range end
     * @return {@link #ASC} if limit is positive, start is not null and end is null; {@link #DESC} otherwise
     */
    public static Order defaultValue(int limit, Object start, Object end) {
        return limit > 0 && start != null && end == null ? ASC : DESC;
    }

    @Override
    public String toString() {
        return text;
    }
}
