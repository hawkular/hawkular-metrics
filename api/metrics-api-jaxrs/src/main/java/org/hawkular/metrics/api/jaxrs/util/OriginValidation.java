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
package org.hawkular.metrics.api.jaxrs.util;

import static org.hawkular.metrics.api.jaxrs.config.ConfigurationKey.ALLOWED_CORS_ORIGINS;

import java.util.function.Predicate;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.hawkular.jaxrs.filter.cors.OriginPredicate;
import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;

/**
 * @author Stefan Negrea
 */
@ApplicationScoped
@Eager
public class OriginValidation {

    private Predicate<String> predicate;

    @Inject
    @Configurable
    @ConfigurationProperty(ALLOWED_CORS_ORIGINS)
    String allowedCorsOriginsConfig;

    @PostConstruct
    protected void init() {
        predicate = new OriginPredicate(allowedCorsOriginsConfig);
    }

    public Predicate<String> getPredicate() {
        return predicate;
    }
}
