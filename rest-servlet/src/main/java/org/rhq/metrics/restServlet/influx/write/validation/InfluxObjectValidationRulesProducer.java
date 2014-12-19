package org.rhq.metrics.restServlet.influx.write.validation;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import com.google.common.collect.ImmutableList;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class InfluxObjectValidationRulesProducer {

    @Produces
    @ApplicationScoped
    @InfluxObjectRules
    public List<InfluxObjectValidationRule> influxObjectValidationRules() {
        return ImmutableList.of( //
            new HasNameRule(), //
            new SupportedColumnsRule(), //
            new DataTypesRule() //
            );
    }
}
