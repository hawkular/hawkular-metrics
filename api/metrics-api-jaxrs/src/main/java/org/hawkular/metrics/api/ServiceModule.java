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
package org.hawkular.metrics.api;

import java.lang.reflect.Field;

import org.hawkular.metrics.api.jaxrs.config.Configurable;
import org.hawkular.metrics.api.jaxrs.config.ConfigurableProducer;
import org.hawkular.metrics.api.jaxrs.config.ConfigurationProperty;

import com.google.inject.AbstractModule;
import com.google.inject.MembersInjector;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;

public class ServiceModule extends AbstractModule {

    ConfigurableProducer configurableProducer;

    public ServiceModule() {
        configurableProducer = new ConfigurableProducer();
        configurableProducer.init();
    }

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Configurable.class).to(String.class);

        binder().bindListener(Matchers.any(), new TypeListener() {
            @Override
            public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
                for (Field field : type.getRawType().getDeclaredFields()) {
                    if (field.isAnnotationPresent(Configurable.class)) {
                        field.setAccessible(true);
                        encounter.register(new MembersInjector<Object>() {
                            @Override
                            public void injectMembers(Object instance) {
                                try {
                                    Object value = configurableProducer
                                            .getValue(field.getAnnotation(ConfigurationProperty.class));
                                    field.set(instance, value);
                                } catch (IllegalAccessException e) {
                                    binder().addError(e);
                                }
                            }
                        });
                    }
                }
            }
        });
    }
}
