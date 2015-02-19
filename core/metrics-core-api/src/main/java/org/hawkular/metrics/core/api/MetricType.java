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
package org.hawkular.metrics.core.api;

/**
 * An enumeration of the supported metric types which currently includes,
 *
 * <ul>
 *   <li>numeric</li>
 *   <li>availability</li>
 *   <li>log events</li>
 * </ul>
 *
 * @author John Sanda
 */
public enum MetricType {

    NUMERIC(0, "numeric"),

    AVAILABILITY(1, "availability"),

    LOG_EVENT(2, "log event");

    private int code;

    private String text;

    private MetricType(int code, String text) {
        this.code = code;
        this.text = text;
    }

    public int getCode() {
        return code;
    }

    public String getText() {
        return text;
    }

    @Override
    public String toString() {
        return text;
    }

    public static MetricType fromCode(int code) {
        switch (code) {
            case 0 : return NUMERIC;
            case 1 : return AVAILABILITY;
            case 2 : return LOG_EVENT;
            default: throw new IllegalArgumentException(code + " is not a recognized metric type");
        }
    }

    public static MetricType fromTextCode(String textCode) {
        switch (textCode) {
        case "num": return NUMERIC;
        case "avail": return AVAILABILITY;
        case "log": return LOG_EVENT;
        default: throw new IllegalArgumentException(textCode + " is not a recognized metric type code");
        }
    }

}
