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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.Properties;

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

    private EnumMap<ConfigurationKey, String> effectiveConfig;

    @PostConstruct
    void init() {
        effectiveConfig = new EnumMap<>(ConfigurationKey.class);

        Properties configFileProperties = new Properties();
        File configurationFile = findConfigurationFile();
        if (configurationFile != null) {
            load(configFileProperties, configurationFile);
        }

        for (ConfigurationKey configKey : ConfigurationKey.values()) {
            String name = configKey.toString();
            String envName = configKey.toEnvString();

            String value = System.getProperty(name);
            if (value == null && envName != null) {
                value = System.getenv(envName);
            }
            if (value == null) {
                value = configFileProperties.getProperty(name);
            }

            if (configKey.isFlag()) {
                effectiveConfig.put(configKey, String.valueOf(value != null));
            } else {
                effectiveConfig.put(configKey, value != null ? value : configKey.defaultValue());
            }
        }
    }

    @Produces
    @Configurable
    String getConfigurationPropertyAsString(InjectionPoint injectionPoint) {
        ConfigurationProperty configProp = injectionPoint.getAnnotated().getAnnotation(ConfigurationProperty.class);
        if (configProp == null) {
            String message = "Any field or parameter annotated with @" + Configurable.class.getSimpleName()
                             + " must also be annotated with @" + ConfigurationProperty.class.getSimpleName();
            throw new IllegalArgumentException(message);
        }
        return effectiveConfig.get(configProp.value());
    }

    private File findConfigurationFile() {
        String configurationFilePath = System.getProperty(METRICS_CONF);
        if (configurationFilePath != null) {
            File file = new File(configurationFilePath);
            checkExplicitConfigurationFile(file);
            return file;
        }
        File file = new File(System.getProperty("user.home"), ".metrics.conf");
        if (!file.exists()) {
            return null;
        }
        checkConfigurationFile(file);
        return file;
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

    private void load(Properties properties, File file) {
        try (InputStream input = new FileInputStream(file)) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
