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
package org.hawkular.metrics.clients.ptrans.collectd.util;

/**
 * Simple assertion utilities.
 *
 * @author Thomas Segismont
 */
public class Assert {

    /**
     * Checks that <code>o</code> is not null. The error message is formatted with {@link String#format(String,
     * Object...)} if <code>params</code> are not empty.
     *
     * @param o      an object
     * @param msg    error message
     * @param params error message parameters
     *
     * @throws IllegalArgumentException if <code>o</code> is null
     */
    public static void assertNotNull(Object o, String msg, Object... params) {
        if (o == null) {
            throwIllegalArgumentException(msg, params);
        }
    }

    /**
     * Checks that <code>a == b</code>. The error message is formatted with {@link String#format(String, Object...)} if
     * <code>params</code> are not empty.
     *
     * @param a      an int
     * @param b      another int
     * @param msg    error message
     * @param params error message parameters
     *
     * @throws IllegalArgumentException if <code>o</code> is null
     */
    public static void assertEquals(int a, int b, String msg, Object... params) {
        if (a != b) {
            throwIllegalArgumentException(msg, params);
        }
    }

    private static void throwIllegalArgumentException(String msg, Object[] params) {
        if (params != null && params.length > 0) {
            throw new IllegalArgumentException(String.format(msg, params));
        }
        throw new IllegalArgumentException(msg);
    }

    private Assert() {
        // Utility class
    }
}
