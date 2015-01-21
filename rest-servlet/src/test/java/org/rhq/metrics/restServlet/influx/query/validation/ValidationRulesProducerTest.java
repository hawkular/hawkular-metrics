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
package org.rhq.metrics.restServlet.influx.query.validation;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import java.util.List;

import org.junit.Test;

import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class ValidationRulesProducerTest {
    private static final SelectQueryValidationRule FAKE_RULE = new SelectQueryValidationRule() {
        @Override
        public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        }
    };

    private final ValidationRulesProducer rulesProducer = new ValidationRulesProducer();

    @Test
    public void validationRulesShouldBeImmutable() throws Exception {
        List<SelectQueryValidationRule> rules = rulesProducer.selectQueryValidationRules();
        try {
            rules.addAll(rules);
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception ignored) {
        }
        try {
            rules.remove(0);
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception ignored) {
        }
        try {
            rules.clear();
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception ignored) {
        }
        try {
            rules.listIterator().remove();
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception ignored) {
        }
        try {
            rules.set(0, FAKE_RULE);
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception ignored) {
        }
    }
}
