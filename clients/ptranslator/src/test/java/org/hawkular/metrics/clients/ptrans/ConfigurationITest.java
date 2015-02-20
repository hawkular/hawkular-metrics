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

import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hawkular.metrics.clients.ptrans.CanReadMatcher.canRead;
import static org.hawkular.metrics.clients.ptrans.ConfigurationKey.SERVICES;
import static org.hawkular.metrics.clients.ptrans.IsFileMatcher.isFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;

import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * @author Thomas Segismont
 */
public class ConfigurationITest extends ExecutableITestBase {

    @Test
    public void shouldExitWithErrorIfServicesPropertyIsMissing() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.remove(SERVICES.getExternalForm());
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }

        ptransProcessBuilder.command().addAll(Lists.newArrayList("-c", ptransConfFile.getAbsolutePath()));

        ptransProcess = ptransProcessBuilder.start();
        int returnCode = ptransProcess.waitFor();
        assertNotEquals(0, returnCode);

        assertThat(ptransErr, allOf(isFile(), canRead()));

        String expectedContent = String.format("Invalid configuration:%nProperty services not found");
        String actualContent = Files.lines(ptransErr.toPath()).collect(joining(System.getProperty("line.separator")));
        assertEquals(expectedContent, actualContent);
    }

    @Test
    public void shouldExitWithErrorIfServicesListIsEmpty() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(SERVICES.getExternalForm(), "   , , ,,,,   ,,,,   ,");
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }

        ptransProcessBuilder.command().addAll(Lists.newArrayList("-c", ptransConfFile.getAbsolutePath()));

        ptransProcess = ptransProcessBuilder.start();
        int returnCode = ptransProcess.waitFor();
        assertNotEquals(0, returnCode);

        assertThat(ptransErr, allOf(isFile(), canRead()));

        String expectedContent = String.format("Invalid configuration:%nEmpty services list");
        String actualContent = Files.lines(ptransErr.toPath()).collect(joining(System.getProperty("line.separator")));
        assertEquals(expectedContent, actualContent);
    }

    @Test
    public void shouldExitWithErrorIfServicesListContainsUnknownService() throws Exception {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(ptransConfFile)) {
            properties.load(in);
        }
        properties.setProperty(SERVICES.getExternalForm(), "marseille, collectd");
        try (OutputStream out = new FileOutputStream(ptransConfFile)) {
            properties.store(out, "");
        }

        ptransProcessBuilder.command().addAll(Lists.newArrayList("-c", ptransConfFile.getAbsolutePath()));

        ptransProcess = ptransProcessBuilder.start();
        int returnCode = ptransProcess.waitFor();
        assertNotEquals(0, returnCode);

        assertThat(ptransErr, allOf(isFile(), canRead()));

        String expectedContent = String.format("Invalid configuration:%nUnknown service marseille");
        String actualContent = Files.lines(ptransErr.toPath()).collect(joining(System.getProperty("line.separator")));
        assertEquals(expectedContent, actualContent);
    }
}
