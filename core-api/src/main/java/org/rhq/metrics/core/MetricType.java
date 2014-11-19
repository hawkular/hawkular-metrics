package org.rhq.metrics.core;

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

}
