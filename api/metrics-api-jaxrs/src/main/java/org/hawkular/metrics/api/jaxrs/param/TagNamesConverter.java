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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Set;

import javax.ws.rs.ext.ParamConverter;

import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;


/**
 * A JAX-RS {@link ParamConverter} for {@link TagNames} parameters. The string format is a list of tags in the {@code
 * name(:value)} form, comma-separated.
 *
 * @author Thomas Segismont
 */
public class TagNamesConverter implements ParamConverter<TagNames> {

    @Override
    public TagNames fromString(String value) {
        String[] tokens = value.split(",", -1);
        Set<String> names = Arrays.stream(tokens).map(token -> {
            if (token.trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid tag list:" + value);
            }
            String[] parts = token.split(":", -1);
            if (parts.length > 2) {
                throw new IllegalArgumentException("Invalid tag list:" + value);
            }
            String key = parts[0];
            if (key.trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid tag list:" + value);
            }
            return key;
        }).collect(toSet());
        return new TagNames(names);
    }

    @Override
    public String toString(TagNames value) {
        return value.getNames().stream().collect(joining(Tags.LIST_DELIMITER));
    }
}
