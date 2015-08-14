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
package org.hawkular.metrics.api.jaxrs.config;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

/**
 * Qualifier for an injected field or parameter which value should be fetched from configuration. For example:
 * <pre>
 *     &#064;Configurable
 *     &#064;ConfigurationProperty(ConfigurationKey.BACKEND)
 *     &#064;Inject
 *     String backendType;
 * </pre>
 * Configuration is a set of key/value pairs (configuration properties) defined by the following sources, in order of
 * precedence:
 * <ul>
 *     <li>system properties (-Dkey=value)</li>
 *     <li>external {@link java.util.Properties} file, which path is defined by the <em>metrics.conf</em> system
 *     property; by default, <em>&lt;user.home&gt;/.metrics.conf</em> is used</li>
 *     <li>internal {@link java.util.Properties} file (<em>META-INF/metrics.conf</em>)</li>
 * </ul>
 * Any field or parameter annotated with {@link Configurable} must also be annotated with {@link ConfigurationProperty}.
 * The value of the latter specifies the configuration property key. Otherwise the configuration producer method will
 * throw an instance of {@link java.lang.IllegalArgumentException}. In most cases, a configuration property should at
 * least be defined in the internal properties file, in order to provide a default value.<br>
 * <br>
 * Configuration values may be modified at runtime (through JMX). When a bean can/should adapt to such changes, the
 * injected field or parameter type can be wrapped with {@link javax.enterprise.inject.Instance}. The configuration
 * value will be fetched on each call to {@link javax.enterprise.inject.Instance#get()}.
 *
 * @author Thomas Segismont
 * @see ConfigurationProperty
 * @see ConfigurableProducer
 */
@Qualifier
@Retention(RUNTIME)
@Target({ METHOD, FIELD, PARAMETER })
@Documented
public @interface Configurable {
}
