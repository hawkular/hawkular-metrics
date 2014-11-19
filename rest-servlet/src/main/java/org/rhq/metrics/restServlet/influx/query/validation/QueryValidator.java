package org.rhq.metrics.restServlet.influx.query.validation;

import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.rhq.metrics.restServlet.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
@ApplicationScoped
public class QueryValidator {

    private final List<SelectQueryValidationRule> selectQueryValidationRules;

    @Inject
    public QueryValidator(@InfluxSelectQueryRules List<SelectQueryValidationRule> selectQueryValidationRules) {
        this.selectQueryValidationRules = selectQueryValidationRules;
    }

    public void validateSelectQuery(SelectQueryDefinitions definitions) throws IllegalQueryException {
        for (SelectQueryValidationRule rule : selectQueryValidationRules) {
            rule.checkQuery(definitions);
        }
    }
}
