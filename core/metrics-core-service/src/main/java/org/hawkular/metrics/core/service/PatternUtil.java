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

package org.hawkular.metrics.core.service;

import java.util.regex.Pattern;

/**
 * @author Thomas Segismont
 */
public class PatternUtil {

    /**
     * Allow special cases to Pattern matching, such as "*" -> ".*" and ! indicating the match shouldn't
     * happen. The first ! indicates the rest of the pattern should not match.
     *
     * @param inputRegexp Regexp given by the user
     * @return Pattern modified to allow special cases in the query language
     */
    public static Pattern filterPattern(String inputRegexp) {
        if (inputRegexp.equals("*")) {
            inputRegexp = ".*";
        } else if (inputRegexp.startsWith("!")) {
            inputRegexp = inputRegexp.substring(1);
        }
        return Pattern.compile(inputRegexp); // Catch incorrect patterns..
    }

    private PatternUtil() {
        // Utility class
    }
}
