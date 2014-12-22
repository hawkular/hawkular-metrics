package org.rhq.metrics.restServlet.influx.write.validation;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public class SupportedColumnsRule implements InfluxObjectValidationRule {
    @Override
    public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        List<String> columns = influxObject.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new InvalidObjectException("Object has empty columns attribute");
        }
        if (columns.size() == 1) {
            if (!columns.contains("value")) {
                throw new InvalidObjectException("Object has no 'value' column");
            }
        } else if (columns.size() == 2) {
            if (!columns.containsAll(ImmutableList.of("time", "value"))) {
                throw new InvalidObjectException("Object has columns other than 'time' or 'value'");
            }
        } else {
            throw new InvalidObjectException("Object has more than two columns");
        }
    }
}
