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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.ext.ParamConverter;

import org.hawkular.metrics.model.param.Tags;


/**
 * A JAX-RS {@link ParamConverter} for {@link Tags} parameters. The string format is a list of tags in the {@code
 * name:value} form, comma-separated.
 *
 * @author Thomas Segismont
 */
public class TagsConverter implements ParamConverter<Tags> {

    @Override
    public Tags fromString(String value) {
        return convert(value);
    }

    public static Tags fromNullable(String value) {
        if (value == null) {
            return new Tags(Collections.emptyMap());
        }
        return convert(value);
    }

    private static Tags convert(String value) {
        if (value.isEmpty()) {
            return new Tags(Collections.emptyMap());
        }
        checkArgument(!value.trim().isEmpty(), "Invalid tags: %s", value);
        Map<String, String> tags = new HashMap<>();
        String previousToken = null;
        StringTokenizer tokenizer = new StringTokenizer(value, Tags.LIST_DELIMITER, true);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (previousToken == null) {
                checkArgument(!Tags.LIST_DELIMITER.equals(token), "Invalid tags: %s", value);
            } else {
                checkArgument(!Tags.LIST_DELIMITER.equals(previousToken));
            }
            if (Tags.LIST_DELIMITER.equals(token)) {
                previousToken = null;
                continue;
            }
            int colonIndex = token.indexOf(Tags.TAG_DELIMITER);
            checkArgument(hasExpectedForm(token, colonIndex), "Invalid tags: %s", value);
            try {
                String tagValue = URLDecoder.decode(token.substring(colonIndex + 1), StandardCharsets.UTF_8.name());
                tags.put(token.substring(0, colonIndex), tagValue);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            previousToken = token;
        }
        return new Tags(tags);
    }

    private static boolean hasExpectedForm(String token, int colonIndex) {
        return colonIndex > 0 && colonIndex < token.length();
    }

    @Override
    public String toString(Tags value) {
        return value.getTags().entrySet().stream()
                    .map(e -> e.getKey() + Tags.TAG_DELIMITER + e.getValue())
                    .collect(joining(Tags.LIST_DELIMITER));
    }
}
