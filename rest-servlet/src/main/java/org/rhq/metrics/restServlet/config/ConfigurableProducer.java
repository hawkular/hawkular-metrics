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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.inject.Inject;

import org.rhq.metrics.restServlet.management.MBeanRegistrar;

/**
 * @author Thomas Segismont
 * @see Configurable
 */
@ApplicationScoped
public class ConfigurableProducer implements ConfigurationManagement {

    @Inject
    private MBeanRegistrar mBeanRegistrar;

    // Empty if external configuration file should be ignored
    private Optional<File> configurationFile;
    // Value needs to be Optional as ConcurrentHashMap does not allow null values
    private ConcurrentMap<String, Optional<String>> effectiveConfiguration;
    private CopyOnWriteArraySet<String> readSystemProperties;

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
        readSystemProperties = new CopyOnWriteArraySet<>();
        mBeanRegistrar.registerMBean(this);
    }

    @PreDestroy
    void tearDown() {
        mBeanRegistrar.unregisterMBean(this);
    }

    @Produces
    @Configurable
    String getConfigurationPropertyAsString(InjectionPoint injectionPoint) {
        ConfigurationProperty configProp = injectionPoint.getAnnotated().getAnnotation(ConfigurationProperty.class);
        String propertyName = configProp.value().getExternalForm();
        return lookupConfigurationProperty(propertyName);
    }

    private String lookupConfigurationProperty(String propertyName) {
        if (!readSystemProperties.contains(propertyName)) {
            String sysprop = System.getProperty(propertyName);
            if (sysprop != null) {
                effectiveConfiguration.put(propertyName, Optional.of(sysprop));
            }
            readSystemProperties.add(propertyName);
        }
        return effectiveConfiguration.get(propertyName).orElse(null);
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

    @Override
    public String[] getConfigurationPropertyKeys() {
        Set<String> keys = effectiveConfiguration.keySet();
        return keys.toArray(new String[keys.size()]);
    }

    @Override
    public String getConfigurationProperty(String key) {
        return lookupConfigurationProperty(key);
    }

    @Override
    public void updateConfigurationProperty(String key, String value) {
        effectiveConfiguration.put(key, Optional.ofNullable(value));
    }
}
