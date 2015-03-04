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
package org.hawkular.metrics.clients.ptrans;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.hawkular.metrics.clients.ptrans.Service.COLLECTD;

import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Thomas Segismont
 */
public class PTransTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void constructorShouldThrowExceptionWhenProvidedConfigurationIsNull() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(equalTo("Configuration is null"));
        new PTrans(null);
    }

    @Test
    public void constructorShouldThrowExceptionWhenProvidedConfigurationIsInvalid() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Property services not found");
        new PTrans(Configuration.from(new Properties()));
    }

    @Test
    public void constructorShouldNotThrowExceptionWhenProvidedConfigurationIsValid() {
        Properties properties = new Properties();
        properties.setProperty(SERVICES.getExternalForm(), COLLECTD.getExternalForm());
        new PTrans(Configuration.from(properties));
    }

}
