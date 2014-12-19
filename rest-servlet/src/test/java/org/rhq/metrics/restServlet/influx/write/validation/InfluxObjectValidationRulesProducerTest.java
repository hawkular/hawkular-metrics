package org.rhq.metrics.restServlet.influx.write.validation;

import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import java.util.List;

import org.junit.Test;

import org.rhq.metrics.restServlet.influx.InfluxObject;

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
