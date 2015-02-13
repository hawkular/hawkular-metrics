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
package org.hawkular.metrics.api.jaxrs.influx.query.validation;

import static org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.OperandUtils.isInstantOperand;
import static org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.OperandUtils.isTimeOperand;

import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.AndBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.BooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.GtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.LtBooleanExpression;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.Operand;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.definition.SelectQueryDefinitions;

/**
 * @author Thomas Segismont
 */
public class SimpleTimeRangesOnlyRule implements SelectQueryValidationRule {
    @Override
    public void checkQuery(SelectQueryDefinitions queryDefinitions) throws IllegalQueryException {
        BooleanExpression whereClause = queryDefinitions.getWhereClause();
        if (whereClause == null) {
            return;
        }
        if (whereClause instanceof LtBooleanExpression) {
            LtBooleanExpression lt = (LtBooleanExpression) whereClause;
            checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
            checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
        } else if (whereClause instanceof GtBooleanExpression) {
            GtBooleanExpression gt = (GtBooleanExpression) whereClause;
            checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
            checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
        } else if (whereClause instanceof AndBooleanExpression) {
            AndBooleanExpression and = (AndBooleanExpression) whereClause;
            BooleanExpression left = and.getLeftExpression();
            BooleanExpression right = and.getRightExpression();
            if (left instanceof LtBooleanExpression) {
                LtBooleanExpression lt = (LtBooleanExpression) left;
                checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
                checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
                if (right instanceof GtBooleanExpression) {
                    GtBooleanExpression gt = (GtBooleanExpression) right;
                    checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
                    checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
                    checkRestrictionIsARange(lt, gt);
                } else if (right instanceof LtBooleanExpression) {
                    LtBooleanExpression lt2 = (LtBooleanExpression) right;
                    checkOneTimeOperand(lt2.getLeftOperand(), lt2.getRightOperand());
                    checkOneDateOrMomentOperand(lt2.getLeftOperand(), lt2.getRightOperand());
                    checkRestrictionIsARange(lt, lt2);
                } else {
                    throw new QueryNotSupportedException("Not a simple time range restriction");
                }
            } else if (left instanceof GtBooleanExpression) {
                GtBooleanExpression gt = (GtBooleanExpression) left;
                checkOneTimeOperand(gt.getLeftOperand(), gt.getRightOperand());
                checkOneDateOrMomentOperand(gt.getLeftOperand(), gt.getRightOperand());
                if (right instanceof LtBooleanExpression) {
                    LtBooleanExpression lt = (LtBooleanExpression) right;
                    checkOneTimeOperand(lt.getLeftOperand(), lt.getRightOperand());
                    checkOneDateOrMomentOperand(lt.getLeftOperand(), lt.getRightOperand());
                    checkRestrictionIsARange(lt, gt);
                } else if (right instanceof GtBooleanExpression) {
                    GtBooleanExpression gt2 = (GtBooleanExpression) right;
                    checkOneTimeOperand(gt2.getLeftOperand(), gt2.getRightOperand());
                    checkOneDateOrMomentOperand(gt2.getLeftOperand(), gt2.getRightOperand());
                    checkRestrictionIsARange(gt, gt2);
                } else {
                    throw new QueryNotSupportedException("Not a simple time range restriction");
                }
            } else {
                throw new QueryNotSupportedException("Not a simple time range restriction");
            }
        } else {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkRestrictionIsARange(LtBooleanExpression lt, GtBooleanExpression gt)
        throws QueryNotSupportedException {

        // Don't allow "time < y and z > time" or "z > time and time < y"
        if ((isTimeOperand(lt.getLeftOperand()) && isTimeOperand(gt.getRightOperand()))
            || (isTimeOperand(lt.getRightOperand()) && isTimeOperand(gt.getLeftOperand()))) {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkRestrictionIsARange(GtBooleanExpression gt1, GtBooleanExpression gt2)
        throws QueryNotSupportedException {

        // Don't allow "time > y and time > z" or "y > time and z > time"
        if ((isTimeOperand(gt1.getLeftOperand()) && isTimeOperand(gt2.getLeftOperand()))
            || (isTimeOperand(gt1.getRightOperand()) && isTimeOperand(gt2.getRightOperand()))) {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkRestrictionIsARange(LtBooleanExpression lt1, LtBooleanExpression lt2)
        throws QueryNotSupportedException {

        // Don't allow "time < y and time < z" or "y < time and z < time"
        if ((isTimeOperand(lt1.getLeftOperand()) && isTimeOperand(lt2.getLeftOperand()))
            || (isTimeOperand(lt1.getRightOperand()) && isTimeOperand(lt2.getRightOperand()))) {
            throw new QueryNotSupportedException("Not a simple time range restriction");
        }
    }

    private void checkOneTimeOperand(Operand leftOperand, Operand rightOperand) throws QueryNotSupportedException {
        // We want exactly one of the operands to be a time operand
        if (isTimeOperand(leftOperand) == isTimeOperand(rightOperand)) {
            throw new QueryNotSupportedException("Expected exactly one time operand");
        }
    }

    private void checkOneDateOrMomentOperand(Operand leftOperand, Operand rightOperand)
        throws QueryNotSupportedException {

        // We want exactly one of the operands to be a date or moment operand
        if (isInstantOperand(leftOperand) == isInstantOperand(rightOperand)) {
            throw new QueryNotSupportedException("Expected exactly one time operand");
        }
    }
}
