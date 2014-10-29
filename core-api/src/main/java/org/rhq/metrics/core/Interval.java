package org.rhq.metrics.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Describes an interval or duration intended to be used with aggregated metrics. An interval describes the frequency
 * at which aggregated metrics are computed/updated. It consists of two parts - length and units. Examples include,
 *
 * <ul>
 *   <li>1min</li>
 *   <li>5min</li>
 *   <li>1hr</li>
 *   <li>5hr</li>
 *   <li>1d</li>
 *   <li>5d</li>
 * </ul>
 *
 * where <i>min</i> denotes minute, <i>hr</i> denotes hour, and <i>d</i> denotes day.
 *
 * @author John Sanda
 */
public class Interval {

    private static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d+)(min|hr|d)");

    public static enum Units {
        MINUTES("min"),

        HOURS("hr"),

        DAYS("d");

        private String code;

        private Units(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }

        public static Units fromCode(String code) {
            switch (code) {
                case "min": return MINUTES;
                case "hr": return HOURS;
                case "d": return DAYS;
                default: throw new IllegalArgumentException(code + " is not a recognized unit");
            }
        }
    }

    public static final Interval NONE = new Interval(0, null) {
        @Override
        public String toString() {
            return "";
        }
    };

    private int length;

    private Units units;

    public Interval(int length, Units units) {
        this.length = length;
        this.units = units;
    }

    /**
     * Parses the string into an interval. The string must match the regular expression (\d+)(min|hr|d); otherwise,
     * an exception is thrown.
     *
     * @param s The string to parse
     * @return The {@link Interval}
     * @throws java.lang.IllegalArgumentException if the string does not parse
     */
    public static Interval parse(String s) {
        Matcher matcher = INTERVAL_PATTERN.matcher(s);
        if (!(matcher.matches() && matcher.groupCount() == 2)) {
            throw new IllegalArgumentException(s + " is not a valid interval. It must follow the pattern " +
                INTERVAL_PATTERN.pattern());
        }
        return new Interval(Integer.parseInt(matcher.group(1)), Units.fromCode(matcher.group(2)));
    }


    public int getLength() {
        return length;
    }

    public Units getUnits() {
        return units;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Interval interval = (Interval) o;

        if (length != interval.length) return false;
        if (units != interval.units) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = length;
        result = 31 * result + units.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return length + units.code;
    }
}
