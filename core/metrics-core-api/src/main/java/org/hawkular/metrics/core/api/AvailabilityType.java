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

import java.nio.ByteBuffer;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public enum AvailabilityType {

    UP((byte) 0, "up"),

    DOWN((byte) 1, "down");

    private byte code;

    private String text;

    private AvailabilityType(byte code, String text) {
        this.code = code;
        this.text = text;
    }

    public byte getCode() {
        return code;
    }

    @JsonValue
    public String getText() {
        return text;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("code", code).add("text", text).toString();
    }

    public static AvailabilityType fromString(String s) {
        switch (s.toLowerCase()) {
            case "up" : return UP;
            case "down" : return DOWN;
            default: throw new IllegalArgumentException(s + " is not a recognized availability type");
        }
    }

    public static AvailabilityType fromBytes(ByteBuffer bytes) {
        switch (bytes.array()[bytes.position()]) {
        case 0 : return UP;
        case 1 : return DOWN;
        default: throw new IllegalArgumentException(bytes.array()[0] + " is not a recognized availability type");
        }
    }
}
