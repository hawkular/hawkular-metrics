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

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Provides a management bean, attribute, operation or parameter description. On a management interface, it may be used
 * to annotate:
 * <ul>
 *     <li>the type (MBean description)</li>
 *     <li>a getter method (attribute description)</li>
 *     <li>a setter method (write-only attribute description)</li>
 *     <li>a method (operation description)</li>
 *     <li>a method parameter (operation parameter description)</li>
 * </ul>
 *
 * @author Thomas Segismont
 * @see org.rhq.metrics.restServlet.management.MBean
 */
@Retention(value = RUNTIME)
@Target({ TYPE, METHOD, PARAMETER })
@Documented
public @interface Description {
    /**
     * @return the bean, attribute, operation or parameter description
     */
    String value();
}
