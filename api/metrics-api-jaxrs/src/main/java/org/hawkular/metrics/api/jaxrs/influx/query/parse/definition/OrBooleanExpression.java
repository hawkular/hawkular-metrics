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
package org.hawkular.metrics.api.jaxrs.influx.query.parse.definition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Thomas Segismont
 */
public class OrBooleanExpression implements BooleanExpression {

    private final BooleanExpression leftExpression;
    private final BooleanExpression rightExpression;
    private final List<BooleanExpression> children;

    public OrBooleanExpression(BooleanExpression leftExpression, BooleanExpression rightExpression) {
        this.leftExpression = leftExpression;
        this.rightExpression = rightExpression;
        children = Collections.unmodifiableList(Arrays.asList(leftExpression, rightExpression));
    }

    public BooleanExpression getLeftExpression() {
        return leftExpression;
    }

    public BooleanExpression getRightExpression() {
        return rightExpression;
    }

    @Override
    public boolean hasChildren() {
        return true;
    }

    @Override
    public List<BooleanExpression> getChildren() {
        return children;
    }
}
