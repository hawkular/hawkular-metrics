package org.rhq.metrics.restServlet.influx.write.validation;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Predicates.or;

import java.math.BigDecimal;
import java.util.List;
import java.util.ListIterator;

import com.google.common.base.Predicate;

import org.rhq.metrics.restServlet.influx.InfluxObject;

/**
 * @author Thomas Segismont
 */
public class DataTypesRule implements InfluxObjectValidationRule {
    private static final Predicate<Object> IS_INTEGRAL = //
    and( //
        instanceOf(Number.class), //
        not( //
            or( //
                instanceOf(BigDecimal.class), //
                instanceOf(Double.class), //
                instanceOf(Float.class) //
            ) //
        ) //
    );

    @Override
    public void checkInfluxObject(InfluxObject influxObject) throws InvalidObjectException {
        List<List<?>> points = influxObject.getPoints();
        if (points == null) {
            return;
        }
        List<String> columns = influxObject.getColumns();
        for (List<?> point : points) {
            if (point.size() < columns.size()) {
                throw new InvalidObjectException("Object has not enough data in point to match columns");
            }
            if (columns.size() == 1) {
                if (!(point.get(0) instanceof Number)) {
                    throw new InvalidObjectException("Point 'value' is not numerical");
                }
            } else {
                int valueIndex = columns.indexOf("value");
                for (ListIterator<?> dataIterator = point.listIterator(); dataIterator.hasNext();) {
                    Object data = dataIterator.next();
                    if (valueIndex == dataIterator.nextIndex() - 1) {
                        if (!(data instanceof Number)) {
                            throw new InvalidObjectException("Point 'value' is not numerical");
                        }
                    } else {
                        if (!IS_INTEGRAL.apply(data)) {
                            throw new InvalidObjectException("Point 'time' is not integral");
                        }
                    }
                }
            }
        }
    }
}
