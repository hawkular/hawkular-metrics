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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

/**
 * @author Thomas Segismont
 * @see Configurable
 */
@ApplicationScoped
public class ConfigurableProducer {
    static final String METRICS_CONF = "metrics.conf";

    // Values need to be Optional as ConcurrentHashMap does not accept null values
    private ConcurrentMap<String, Optional<String>> effectiveConfiguration;

    @PostConstruct
    void init() {
        effectiveConfiguration = new ConcurrentHashMap<>(ConfigurationKey.values().length);

        Properties defaultProperties = defaultProperties();
        Properties fileProperties = configurationFile().map(this::configurationFileProperties).orElse(new Properties());

        for (ConfigurationKey configurationKey : ConfigurationKey.values()) {
            String k = configurationKey.getExternalForm();
            String v = System.getProperty(k);
            if (v == null) {
                v = fileProperties.getProperty(k);
                if (v == null) {
                    v = defaultProperties.getProperty(k);
                }
            }
            effectiveConfiguration.put(k, Optional.ofNullable(v));
        }
    }

    @Produces
    @Configurable
    String getConfigurationPropertyAsString(InjectionPoint injectionPoint) {
        ConfigurationProperty configProp = injectionPoint.getAnnotated().getAnnotation(ConfigurationProperty.class);
        if (configProp == null) {
            String message = "Any field or parameter annotated with @Configurable "
                + "must also be annotated with @ConfigurationProperty";
            throw new IllegalArgumentException(message);
        }
        String propertyName = configProp.value().getExternalForm();
        return lookupConfigurationProperty(propertyName);
    }

    private String lookupConfigurationProperty(String propertyName) {
        return effectiveConfiguration.get(propertyName).orElse(null);
    }

    private Optional<File> configurationFile() {
        String configurationFilePath = System.getProperty(METRICS_CONF);
        if (configurationFilePath != null) {
            File file = new File(configurationFilePath);
            checkExplicitConfigurationFile(file);
            return Optional.of(file);
        }
        File file = new File(System.getProperty("user.home"), ".metrics.conf");
        if (!file.exists()) {
            return Optional.empty();
        }
        checkConfigurationFile(file);
        return Optional.of(file);
    }

    private void checkExplicitConfigurationFile(File file) {
        if (!file.exists()) {
            throw new IllegalArgumentException(file + " does not exist");
        }
        checkConfigurationFile(file);
    }

    private void checkConfigurationFile(File file) {
        if (!file.isFile()) {
            throw new IllegalArgumentException(file + " is not a regular file");
        }
        if (!file.canRead()) {
            throw new IllegalArgumentException(file + " is not readable");
        }
    }

    private Properties defaultProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("META-INF/metrics.conf")) {
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Properties configurationFileProperties(File file) {
        try (FileInputStream input = new FileInputStream(file)) {
            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
