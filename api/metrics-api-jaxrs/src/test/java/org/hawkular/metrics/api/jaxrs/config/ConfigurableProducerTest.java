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
package org.hawkular.metrics.api.jaxrs.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import javax.enterprise.inject.spi.Annotated;
import javax.enterprise.inject.spi.InjectionPoint;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author Thomas Segismont
 */
@RunWith(MockitoJUnitRunner.class)
public class ConfigurableProducerTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private InjectionPoint parameterInjectionPoint;
    @Mock
    private InjectionPoint flagInjectionPoint;

    private final ConfigurableProducer configurableProducer = new ConfigurableProducer();

    @Before
    public void before() throws IOException {
        parameterInjectionPoint = configureInjectionPoint(ConfigurationKey.CASSANDRA_KEYSPACE);
        flagInjectionPoint = configureInjectionPoint(ConfigurationKey.CASSANDRA_RESETDB);
    }

    private InjectionPoint configureInjectionPoint(ConfigurationKey configurationKey) {
        Annotated annotated = mock(Annotated.class);
        ConfigurationProperty configurationProperty = mock(ConfigurationProperty.class);
        when(configurationProperty.value()).thenReturn(configurationKey);
        when(annotated.getAnnotation(eq(ConfigurationProperty.class))).thenReturn(configurationProperty);
        InjectionPoint injectionPoint = mock(InjectionPoint.class);
        when(injectionPoint.getAnnotated()).thenReturn(annotated);
        return injectionPoint;
    }

    @After
    public void after() {
        System.clearProperty(ConfigurableProducer.METRICS_CONF);
        System.clearProperty(ConfigurationKey.CASSANDRA_KEYSPACE.toString());
        System.clearProperty(ConfigurationKey.CASSANDRA_RESETDB.toString());
    }

    @Test
    public void shouldFetchDefaultValue() throws Exception {
        // Create an empty config file in case the user running tests has a config file in <user.home>
        File configFile = tempFolder.newFile();
        new Properties().store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(parameterInjectionPoint);

        assertEquals(ConfigurationKey.CASSANDRA_KEYSPACE.defaultValue(), value);
    }

    @Test
    public void shouldFetchValueFromConfigFile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.toString(), "marseille");
        File configFile = tempFolder.newFile();
        properties.store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(parameterInjectionPoint);

        assertEquals("marseille", value);
    }

    @Test
    public void shouldFetchValueFromSystemProperty() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.toString(), "marseille");
        File configFile = tempFolder.newFile();
        properties.store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        System.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.toString(), "mare nostrum");

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(parameterInjectionPoint);

        assertEquals("mare nostrum", value);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfAnnotationIsMissing() throws Exception {
        // Override default: simulate a missing config property annotation
        Annotated annotated = mock(Annotated.class);
        when(annotated.getAnnotation(eq(ConfigurationProperty.class))).thenReturn(null);
        InjectionPoint injectionPoint = mock(InjectionPoint.class);
        when(injectionPoint.getAnnotated()).thenReturn(annotated);

        expectedException.expect(IllegalArgumentException.class);
        String message = "Any field or parameter annotated with @Configurable "
                         + "must also be annotated with @ConfigurationProperty";
        expectedException.expectMessage(message);
        configurableProducer.getConfigurationPropertyAsString(injectionPoint);
    }

    @Test
    public void shouldInjectTrueWhenFlagIsPresent() throws Exception {
        System.setProperty(ConfigurationKey.CASSANDRA_RESETDB.toString(), "");

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(flagInjectionPoint);

        assertTrue(Boolean.parseBoolean(value));
    }

    @Test
    public void shouldInjectFalseWhenFlagIsAbsent() throws Exception {
        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(flagInjectionPoint);

        assertFalse(Boolean.parseBoolean(value));
    }
}
