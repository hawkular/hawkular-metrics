package org.rhq.metrics.restServlet.influx.query.parse.definition;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Thomas Segismont
 */
public enum InfluxTimeUnit {
    /** */
    MICROSECONDS('u') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.MICROSECONDS);
        }
    },
    /** */
    SECONDS('s') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.SECONDS);
        }
    },
    /** */
    MINUTES('m') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.MINUTES);
        }
    },
    /** */
    HOURS('h') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.HOURS);
        }
    },
    /** */
    DAYS('d') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(value, TimeUnit.DAYS);
        }
    },
    /** */
    WEEKS('w') {
        @Override
        public long convertTo(TimeUnit targetUnit, long value) {
            return targetUnit.convert(7 * value, TimeUnit.DAYS);
        }
    };

    private final char id;

    InfluxTimeUnit(char id) {
        if (Character.isUpperCase(id)) {
            this.id = Character.toLowerCase(id);
        } else {
            this.id = id;
        }
    }

    public char getId() {
        return id;
    }

    /**
     * Converts the given <code>value</code> given in <code>this</code> {@link InfluxTimeUnit} to the
     * <code>target</code> {@link java.util.concurrent.TimeUnit}.
     *
     * @param targetUnit the target {@link java.util.concurrent.TimeUnit}
     * @param value the value in <code>this</code> {@link InfluxTimeUnit}
     * @return the value in the target {@link java.util.concurrent.TimeUnit}
     */
    public abstract long convertTo(TimeUnit targetUnit, long value);

    private static final Map<Character, InfluxTimeUnit> UNIT_BY_ID = new HashMap<Character, InfluxTimeUnit>();

    static {
        for (InfluxTimeUnit influxTimeUnit : values()) {
            UNIT_BY_ID.put(influxTimeUnit.id, influxTimeUnit);
        }
    }

    /**
     * @param id time unit id
     * @return the {@link InfluxTimeUnit} which id is <code>id</code>, null otherwise
     */
    public static InfluxTimeUnit findById(char id) {
        return UNIT_BY_ID.get(id);
    }
}
