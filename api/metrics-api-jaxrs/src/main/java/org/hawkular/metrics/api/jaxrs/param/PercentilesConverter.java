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
package org.hawkular.metrics.api.jaxrs.param;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.ws.rs.ext.ParamConverter;

import org.hawkular.metrics.model.Percentile;
import org.hawkular.metrics.model.param.Percentiles;


/**
 * A JAX-RS ParamConverter from number,number,number input string to a Percentiles or vice-versa.
 *
 * @author Michael Burman
 */
public class PercentilesConverter implements ParamConverter<Percentiles> {
    @Override
    public Percentiles fromString(String param) {
        return new Percentiles(Arrays.stream(param.split(",")).map(Percentile::new).collect(Collectors.toList()));
    }

    @Override
    public String toString(Percentiles percentiles) {
        return percentiles.getPercentiles().stream().map(Percentile::getOriginalQuantile)
                .collect(Collectors.joining(","));
    }
}
