package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import com.google.common.collect.ImmutableList;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class ValidationRulesProducer {

    @Produces
    @ApplicationScoped
    @InfluxSelectQueryRules
    public List<SelectQueryValidationRule> selectQueryValidationRules() {
        return ImmutableList.of( //
            new MetricNameRule(), //
            new AggregatorsRule(), //
            new GroupByTimeRule(), //
            new OnlyOneColumnRule(), //
            new SimpleTimeRangesOnlyRule() //
            );
    }
}
