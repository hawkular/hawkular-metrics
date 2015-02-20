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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
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

import com.google.common.io.Resources;

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
    private InjectionPoint injectionPoint;

    private final ConfigurableProducer configurableProducer = new ConfigurableProducer();

    @Before
    public void before() throws IOException {
        Annotated annotated = mock(Annotated.class);
        ConfigurationProperty configurationProperty = mock(ConfigurationProperty.class);
        when(configurationProperty.value()).thenReturn(ConfigurationKey.CASSANDRA_KEYSPACE);
        when(annotated.getAnnotation(eq(ConfigurationProperty.class))).thenReturn(configurationProperty);
        injectionPoint = mock(InjectionPoint.class);
        when(injectionPoint.getAnnotated()).thenReturn(annotated);
    }

    @After
    public void after() {
        System.clearProperty(ConfigurableProducer.METRICS_CONF);
        System.clearProperty(ConfigurationKey.CASSANDRA_KEYSPACE.getExternalForm());
    }

    @Test
    public void shouldFetchDefaultValue() throws Exception {
        // Create an empty config file in case the user running tests has a config file in <user.home>
        Properties emptyProperties = new Properties();
        File configFile = tempFolder.newFile();
        emptyProperties.store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(injectionPoint);

        Properties properties = new Properties();
        URL resource = Resources.getResource("META-INF/metrics.conf");
        properties.load(Resources.asByteSource(resource).openStream());

        assertEquals(properties.getProperty(ConfigurationKey.CASSANDRA_KEYSPACE.getExternalForm()), value);
    }

    @Test
    public void shouldFetchValueFromConfigFile() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.getExternalForm(), "marseille");
        File configFile = tempFolder.newFile();
        properties.store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(injectionPoint);

        assertEquals("marseille", value);
    }

    @Test
    public void shouldFetchValueFromSystemProperty() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.getExternalForm(), "marseille");
        File configFile = tempFolder.newFile();
        properties.store(new FileOutputStream(configFile), null);
        System.setProperty(ConfigurableProducer.METRICS_CONF, configFile.getAbsolutePath());

        System.setProperty(ConfigurationKey.CASSANDRA_KEYSPACE.getExternalForm(), "mare nostrum");

        configurableProducer.init();
        String value = configurableProducer.getConfigurationPropertyAsString(injectionPoint);

        assertEquals("mare nostrum", value);
    }

    @Test
    public void shouldThrowsIllegalArgumentExceptionIfAnnotationIsMissing() throws Exception {
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
}
