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
package org.hawkular.metrics.api.jaxrs.util;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.ws.rs.NameBinding;

/**
 * Annotation that can be applied to a JAX-RS resource class (i.e., handler) or method to enable logging of HTTP
 * requests for REST endpoints. The
 * {@link org.hawkular.metrics.api.jaxrs.config.ConfigurationKey#REQUEST_LOGGING_LEVEL REQUEST_LOGGING_LEVEL} property
 * must be set to enable logging for most endpoints. The value should a logging level, e.g., TRACE, DEBUG, INFO, WARN,
 * ERROR, or FATAL. The value should be upper case. If the value does not match any of the logging levels, then DEBUG is
 * used. The
 * {@link org.hawkular.metrics.api.jaxrs.config.ConfigurationKey#REQUEST_LOGGING_LEVEL_WRITES REQUEST_LOGGING_LEVEL_WRITES}
 * property must be set to enable logging for endpoints that write data points. Its value should also be an upper case
 * logging level.
 *
 * @author jsanda
 */
@NameBinding
@Retention(RUNTIME)
@Target({TYPE, METHOD})
public @interface Logged {
}
