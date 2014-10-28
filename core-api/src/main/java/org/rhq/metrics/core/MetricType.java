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

    NUMERIC("num", "numeric"),

    AVAILABILITY("avail", "availability"),

    LOG_EVENT("log", "log event");

    private String code;

    private String display;

    private MetricType(String code, String display) {
        this.code = code;
        this.display = display;
    }

    public String getCode() {
        return code;
    }


    @Override
    public String toString() {
        return display;
    }

    public static MetricType fromCode(String code) {
        switch (code) {
            case "num" : return NUMERIC;
            case "avail" : return AVAILABILITY;
            case "log" : return LOG_EVENT;
            default: throw new IllegalArgumentException(code + " is not a recognized metric type");
        }
    }

}
