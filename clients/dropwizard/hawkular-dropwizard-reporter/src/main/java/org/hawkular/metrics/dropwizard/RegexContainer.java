/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.dropwizard;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * @author Joel Takvorian
 */
class RegexContainer<T> {
    private final Pattern regex;
    private final T content;

    RegexContainer(Pattern regex, T content) {
        this.regex = regex;
        this.content = content;
    }

    static <T> Optional<RegexContainer<T>> checkAndCreate(String maybeRegex, T content) {
        if (maybeRegex.startsWith("/") && maybeRegex.endsWith("/")) {
            try {
                Pattern regex = Pattern.compile(maybeRegex.substring(1, maybeRegex.length() - 1));
                return Optional.of(new RegexContainer<>(regex, content));
            } catch (PatternSyntaxException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    Optional<T> match(String metricName) {
        if (regex.matcher(metricName).find()) {
            return Optional.of(content);
        }
        return Optional.empty();
    }
}
