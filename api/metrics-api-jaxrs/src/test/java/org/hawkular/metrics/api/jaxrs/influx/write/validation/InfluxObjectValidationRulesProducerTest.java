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

import static org.junit.Assert.fail;

import java.util.List;

import org.hawkular.metrics.api.jaxrs.influx.InfluxObject;
import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class InfluxObjectValidationRulesProducerTest {
    private static final InfluxObjectValidationRule FAKE_RULE = new InfluxObjectValidationRule() {
        @Override
        public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        }
    };

    private final InfluxObjectValidationRulesProducer rulesProducer = new InfluxObjectValidationRulesProducer();

    @Test
    public void validationRulesShouldBeImmutable() throws Exception {
        List<InfluxObjectValidationRule> rules = rulesProducer.influxObjectValidationRules();

        failIfNoExceptionThrown(() -> rules.addAll(rules));
        failIfNoExceptionThrown(() -> rules.remove(0));
        failIfNoExceptionThrown(() -> rules.clear());
        failIfNoExceptionThrown(() -> rules.listIterator().remove());
        failIfNoExceptionThrown(() -> rules.set(0, FAKE_RULE));
    }

    private void failIfNoExceptionThrown(Runnable test) {
        try {
            test.run();
            fail("Expected exception (immutable list modification)");
        } catch (Exception ignored) {

        }
    }
}
