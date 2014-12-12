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
