/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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

package org.hawkular.metrics.api.jaxrs.influx.query.translate;

import static org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.OperandUtils.isTimeOperand;

import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import org.hawkular.metrics.api.jaxrs.influx.InfluxTimeUnit;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AndBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.BooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.InstantOperand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.LongOperand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.LtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.Operand;
import org.joda.time.Instant;
import org.joda.time.Interval;

/**
 * @author Thomas Segismont
 * @deprecated as of 0.17
 */
@Deprecated
@ApplicationScoped
public class ToIntervalTranslator {

    /**
     * Return the interval described by the where clause.
     *
     * @param whereClause the Influx query where clause
     *
     * @return an {@link org.joda.time.Interval} or null if the where clause does define a proper interval (i.e.
     * <code>time &gt; '2012-08-15' and time &lt; '1998-08-15'</code>
     */
    public Interval toInterval(BooleanExpression whereClause) {
        if (whereClause instanceof GtBooleanExpression) {
            return getIntervalFromGtExpression((GtBooleanExpression) whereClause);
        } else if (whereClause instanceof LtBooleanExpression) {
            return getIntervalFromLtExpression((LtBooleanExpression) whereClause);
        }
        AndBooleanExpression and = (AndBooleanExpression) whereClause;
        Interval left;
        Interval right;
        if (and.getLeftExpression() instanceof GtBooleanExpression) {
            left = getIntervalFromGtExpression((GtBooleanExpression) and.getLeftExpression());
        } else {
            left = getIntervalFromLtExpression((LtBooleanExpression) and.getLeftExpression());
        }
        if (and.getRightExpression() instanceof GtBooleanExpression) {
            right = getIntervalFromGtExpression((GtBooleanExpression) and.getRightExpression());
        } else {
            right = getIntervalFromLtExpression((LtBooleanExpression) and.getRightExpression());
        }
        return left.overlap(right);
    }

    private Interval getIntervalFromLtExpression(LtBooleanExpression whereClause) {
        LtBooleanExpression lt = (LtBooleanExpression) whereClause;
        if (isTimeOperand(lt.getLeftOperand())) {
            // time < x
            Instant instant = getInstant(lt.getRightOperand());
            return new Interval(new Instant(0), instant);
        } else {
            // x < time
            Instant instant = getInstant(lt.getLeftOperand());
            return new Interval(instant, Instant.now());
        }
    }

    private Interval getIntervalFromGtExpression(GtBooleanExpression gt) {
        if (isTimeOperand(gt.getLeftOperand())) {
            // time > x
            Instant instant = getInstant(gt.getRightOperand());
            return new Interval(instant, Instant.now());
        } else {
            // x > time
            Instant instant = getInstant(gt.getLeftOperand());
            return new Interval(new Instant(0), instant);
        }
    }

    private Instant getInstant(Operand operand) {
        if (operand instanceof InstantOperand) {
            InstantOperand instantOperand = (InstantOperand) operand;
            return instantOperand.getInstant();
        }
        LongOperand longOperand = (LongOperand) operand;
        return new Instant(InfluxTimeUnit.MICROSECONDS.convertTo(TimeUnit.MILLISECONDS, longOperand.getValue()));
    }
}
