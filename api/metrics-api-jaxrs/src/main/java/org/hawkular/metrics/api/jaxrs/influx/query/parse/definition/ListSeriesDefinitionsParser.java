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

import org.antlr.v4.runtime.tree.TerminalNode;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryBaseListener;
import org.hawkular.metrics.api.jaxrs.influx.query.parse.InfluxQueryParser;

/**
 * @author Thomas Segismont
 */
public class ListSeriesDefinitionsParser extends InfluxQueryBaseListener {
    private RegularExpression regularExpression;

    @Override
    public void exitListSeries(InfluxQueryParser.ListSeriesContext ctx) {
        TerminalNode regexp = ctx.REGEXP();
        if (regexp == null) {
            return;
        }
        String text = regexp.getText();
        boolean caseSensitive = text.charAt(text.length() - 1) == '/';
        String expression = text.substring(1, text.length() - (caseSensitive ? 1 : 2));
        regularExpression = new RegularExpression(expression, caseSensitive);
    }

    public RegularExpression getRegularExpression() {
        return regularExpression;
    }
}
