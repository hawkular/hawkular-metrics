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
package org.hawkular.metrics.api.jaxrs.influx.write.validation;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

import org.hawkular.metrics.api.jaxrs.influx.InfluxObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class UnsupportedInfluxObjectTest {

    @Parameters(name = "unsupportedInfluxObject: {1}")
    public static Iterable<Object[]> testSupportedObjects() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        URL resource = Resources.getResource("influx/write/unsupported-write-objects.list.json");
        return FluentIterable //
            .from(Resources.readLines(resource, Charset.forName("UTF-8"))) //
            // Filter out comment lines
            .filter(new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return !input.startsWith("//") && !input.trim().isEmpty();
                }
            }) //
            .transform(new Function<String, Object[]>() {
                @Override
                public Object[] apply(String input) {
                    try {
                        return new Object[] { mapper.readValue(input, InfluxObject[].class), input };
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final InfluxObjectValidationRulesProducer rulesProducer = new InfluxObjectValidationRulesProducer();
    private final InfluxObjectValidator influxObjectValidator;

    private final InfluxObject[] influxObjects;

    public UnsupportedInfluxObjectTest(InfluxObject[] influxObjects, String objectsAsText) {
        this.influxObjects = influxObjects;
        influxObjectValidator = new InfluxObjectValidator();
        influxObjectValidator.validationRules = rulesProducer.influxObjectValidationRules();
    }

    @Test
    public void unsupportedObjectsShouldFailValidation() throws Exception {
        exception.expect(InvalidObjectException.class);
        influxObjectValidator.validateInfluxObjects(Lists.newArrayList(influxObjects));
    }
}
