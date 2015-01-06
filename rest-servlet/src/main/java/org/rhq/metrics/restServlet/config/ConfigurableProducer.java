/*
 * Copyright 2015 Red Hat, Inc.
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

package org.rhq.metrics.restServlet.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
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
    // Empty if external configuration file should be ignored
    private Optional<File> configurationFile;
    // Value needs to be Optional as ConcurrentHashMap does not allow null values
    private ConcurrentMap<String, Optional<String>> effectiveConfiguration;

    @PostConstruct
    void init() {
        evalConfigurationFile();
        Map<String, String> defaultProperties = loadDefaultProperties();
        effectiveConfiguration = new ConcurrentHashMap<>(defaultProperties.size());
        defaultProperties.forEach((k, v) -> effectiveConfiguration.put(k, Optional.ofNullable(v)));
        configurationFile.ifPresent(file -> {
            Map<String, String> configurationFileProperties = loadConfigurationFileProperties(file);
            configurationFileProperties.forEach((k, v) -> effectiveConfiguration.put(k, Optional.ofNullable(v)));
        });
    }

    @Produces
    @Configurable
    String getConfigurationPropertyAsString(InjectionPoint injectionPoint) {
        return lookupConfigurationProperty(injectionPoint);
    }

    private String lookupConfigurationProperty(InjectionPoint injectionPoint) {
        String propertyName = getConfigurationPropertyName(injectionPoint);
        Optional<String> value = effectiveConfiguration.computeIfAbsent(propertyName,
            key -> Optional.ofNullable(System.getProperty(key)));
        return value.orElse(null);
    }

    private String getConfigurationPropertyName(InjectionPoint injectionPoint) {
        ConfigurationProperty annotation = injectionPoint.getAnnotated().getAnnotation(ConfigurationProperty.class);
        return annotation.value().getExternalForm();
    }

    private void evalConfigurationFile() {
        String configurationFilePath = System.getProperty("metrics.conf");
        if (configurationFilePath != null) {
            File file = new File(configurationFilePath);
            checkExplicitConfigurationFile(file);
            configurationFile = Optional.of(file);
        } else {
            File file = new File(System.getProperty("user.home"), ".metrics.conf");
            if (!file.exists()) {
                configurationFile = Optional.empty();
            } else {
                checkConfigurationFile(file);
                configurationFile = Optional.of(file);
            }
        }
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

    private Map<String, String> loadConfigurationFileProperties(File file) {
        try (FileInputStream input = new FileInputStream(file)) {
            return propertiesToMap(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> loadDefaultProperties() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("META-INF/metrics.conf")) {
            return propertiesToMap(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> propertiesToMap(InputStream input) throws IOException {
        Properties properties = new Properties();
        properties.load(input);
        Map<String, String> map = new HashMap<>(properties.size());
        properties.stringPropertyNames().forEach(name -> map.put(name, properties.getProperty(name)));
        return map;
    }
}
