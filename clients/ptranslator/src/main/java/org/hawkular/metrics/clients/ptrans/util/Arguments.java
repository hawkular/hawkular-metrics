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
package org.hawkular.metrics.clients.ptrans.util;

import java.util.Locale;

/**
 * Argument checking utility.
 *
 * @author Thomas Segismont
 */
public class Arguments {

    /**
     * Checks that {@code test} is not null.
     * <p>
     * The error message is formatted with {@link String#format(java.util.Locale, String, Object...)} for the {@link
     * java.util.Locale#ROOT} locale if {@code params} is not empty.
     *
     * @param test   tested expression
     * @param msg    error message
     * @param params error message parameters
     *
     * @throws IllegalArgumentException if {@code test} is null
     */
    public static void checkArgument(boolean test, String msg, Object... params) {
        if (!test) {
            throwIllegalArgumentException(msg, params);
        }
    }

    private static void throwIllegalArgumentException(String msg, Object[] params) {
        if (params != null && params.length > 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, msg, params));
        }
        throw new IllegalArgumentException(msg);
    }

    private Arguments() {
        // Utility class
    }
}
