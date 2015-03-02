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

import java.util.List;
import java.util.Map;

import com.wordnik.swagger.core.filter.SwaggerSpecFilter;
import com.wordnik.swagger.model.ApiDescription;
import com.wordnik.swagger.model.Operation;
import com.wordnik.swagger.model.Parameter;

/**
 * Filter out AsyncResponse from swagger-json
 *
 * @author Michael Burman
 */
public class JaxRsFilter implements SwaggerSpecFilter {
    @Override
    public boolean isOperationAllowed(Operation operation, ApiDescription apiDescription, Map<String, List<String>>
            map, Map<String, String> map1, Map<String, List<String>> map2) {
        return true;
    }

    @Override
    public boolean isParamAllowed(Parameter parameter, Operation operation, ApiDescription apiDescription,
                                  Map<String, List<String>> map, Map<String, String> map1, Map<String, List<String>>
            map2) {
        if(parameter.dataType().equals("AsyncResponse")) {
            return false;
        }
        return true;
    }
}
