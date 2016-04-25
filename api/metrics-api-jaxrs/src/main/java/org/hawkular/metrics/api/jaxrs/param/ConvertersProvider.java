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
package org.hawkular.metrics.api.jaxrs.param;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Percentiles;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;

import com.google.common.collect.ImmutableMap;

/**
 * Provides Metrics {@link javax.ws.rs.ext.ParamConverterProvider} instances.
 *
 * @author Thomas Segismont
 */
@Provider
public class ConvertersProvider implements ParamConverterProvider {
    private final ImmutableMap<Class<?>, ParamConverter<?>> paramConverters;

    public ConvertersProvider() {
        ImmutableMap.Builder<Class<?>, ParamConverter<?>> paramConvertersBuilder = ImmutableMap.builder();
        paramConverters = paramConvertersBuilder
                .put(Duration.class, new DurationConverter())
                .put(Tags.class, new TagsConverter())
                .put(TagNames.class, new TagNamesConverter())
                .put(MetricType.class, new MetricTypeConverter())
                .put(Order.class, new OrderConverter())
                .put(Percentiles.class, new PercentilesConverter())
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
        return (ParamConverter<T>) paramConverters.get(rawType);
    }
}
