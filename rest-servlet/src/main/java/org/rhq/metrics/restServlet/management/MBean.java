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
 * Annotates a <a href="http://docs.oracle.com/javase/tutorial/jmx/mbeans/standard.html">standard MBean</a> or
 * <a href="http://docs.oracle.com/javase/tutorial/jmx/mbeans/mxbeans.html">MXBean</a> interface.
 *
 * @author Thomas Segismont
 * @see org.rhq.metrics.restServlet.management.KeyProperty
 * @see org.rhq.metrics.restServlet.management.Description
 * @see org.rhq.metrics.restServlet.management.Impact
 * @see org.rhq.metrics.restServlet.management.OperationParamName
 */
@Retention(value = RUNTIME)
@Target(value = TYPE)
@Documented
public @interface MBean {
    String DEFAULT_DOMAIN = "org.rhq.metrics";

    /**
     * @return the MBean domain, defaults to {@link #DEFAULT_DOMAIN}
     */
    String domain() default DEFAULT_DOMAIN;

    /**
     * @return the key properties in the object name
     * @see org.rhq.metrics.restServlet.management.KeyProperty
     */
    KeyProperty[] keyProperties();
}
