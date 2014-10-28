package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class Interval {

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

    private int length;

    private Units units;

    public Interval(int length, Units units) {
        this.length = length;
        this.units = units;
    }

    // TODO add support for parsing from strings like 1min, 1hr, 1day
    // public static Interval fromString(String interval) { ... }


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
        return Objects.toStringHelper(this).add("duration", length).add("units", units).toString();
    }
}
