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
package org.hawkular.metrics.api.jaxrs.swagger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.function.Supplier;

import org.hawkular.metrics.model.param.Duration;
import org.hawkular.metrics.model.param.Tags;

import io.swagger.converter.ModelConverter;
import io.swagger.converter.ModelConverterContext;
import io.swagger.converter.ModelConverters;
import io.swagger.core.filter.AbstractSpecFilter;
import io.swagger.models.Model;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.StringProperty;

/**
 * Swagger hook. This class is not included in the WAR package. It's only used to customize Swagger JSON output.
 *
 * @author Thomas Segismont
 */
public class SwaggerFilter extends AbstractSpecFilter {

    public SwaggerFilter() {
        ModelConverters modelConverters = ModelConverters.getInstance();
        modelConverters.addConverter(new ParamModelConverter(Tags.class, () -> {
            return new StringFormatProperty("tag-list");
        }));
        modelConverters.addConverter(new ParamModelConverter(Duration.class, () -> {
            return new StringFormatProperty("duration");
        }));
    }

    private static class ParamModelConverter implements ModelConverter {
        private final Class<?> paramClass;
        private final Supplier<Property> propertySupplier;

        public ParamModelConverter(Class<?> paramClass, Supplier<Property> propertySupplier) {
            this.paramClass = paramClass;
            this.propertySupplier = propertySupplier;
        }

        @Override
        public Property resolveProperty(Type type, ModelConverterContext context, Annotation[] annotations,
                                        Iterator<ModelConverter> chain) {
            if (type instanceof Class<?>) {
                Class<?> cls = (Class<?>) type;
                if (paramClass.isAssignableFrom(cls)) {
                    return propertySupplier.get();
                }
            }
            if (chain.hasNext()) {
                return chain.next().resolveProperty(type, context, annotations, chain);
            } else {
                return null;
            }
        }

        @Override
        public Model resolve(Type type, ModelConverterContext context, Iterator<ModelConverter> chain) {
            if (chain.hasNext()) {
                return chain.next().resolve(type, context, chain);
            } else {
                return null;
            }
        }

    }

    private static class StringFormatProperty extends StringProperty {
        public StringFormatProperty(String format) {
            setFormat(format);
        }
    }
}
