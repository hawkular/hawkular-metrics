package org.rhq.metrics.core;

import java.nio.ByteBuffer;

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
