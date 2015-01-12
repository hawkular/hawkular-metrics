/*
 * Copyright 2015 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.management;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Declares a key/value pair in an MBean object name. For example, in
 * <em>java.lang:type=GarbageCollector,name=ConcurrentMarkSweep</em>, the key properties are:
 * <ul>
 *     <li><em>type=GarbageCollector</em></li>
 *     <li><em>name=ConcurrentMarkSweep</em></li>
 * </ul>
 *
 * @author Thomas Segismont
 * @see org.rhq.metrics.restServlet.management.MBean
 */
@Retention(value = RUNTIME)
@Target(value = TYPE)
@Documented
public @interface KeyProperty {
    /**
     * @return the key part of this property
     */
    String key();

    /**
     * @return the value part of this property
     */
    String value();
}
