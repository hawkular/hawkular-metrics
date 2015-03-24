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
package org.hawkular.metrics.api.jaxrs.swagger.filter;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.api.jaxrs.param.Duration;
import org.hawkular.metrics.api.jaxrs.param.Tags;

import com.google.common.io.Resources;
import com.wordnik.swagger.converter.ModelConverters;
import com.wordnik.swagger.converter.OverrideConverter;
import com.wordnik.swagger.core.filter.SwaggerSpecFilter;
import com.wordnik.swagger.model.ApiDescription;
import com.wordnik.swagger.model.Operation;
import com.wordnik.swagger.model.Parameter;

/**
 * Swagger configuration.
 *
 * @author Michael Burman
 */
public class JaxRsFilter implements SwaggerSpecFilter {

    public JaxRsFilter() {
        OverrideConverter overrideConverter = new OverrideConverter();
        String durationJson;
        String tagsJson;
        try {
            durationJson = Resources.toString(getResource("rest-doc/duration.json"), UTF_8);
            tagsJson = Resources.toString(getResource("rest-doc/tags.json"), UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        overrideConverter.add(Duration.class.getName(), durationJson);
        overrideConverter.add(Tags.class.getName(), tagsJson);
        ModelConverters.addConverter(overrideConverter, true);
    }

    @Override
    public boolean isOperationAllowed(Operation operation, ApiDescription apiDescription, Map<String, List<String>>
            map, Map<String, String> map1, Map<String, List<String>> map2) {
        return true;
    }

    @Override
    public boolean isParamAllowed(Parameter parameter, Operation operation, ApiDescription apiDescription,
                                  Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>>
            map2) {
        return !parameter.dataType().equals("AsyncResponse");
    }
}
