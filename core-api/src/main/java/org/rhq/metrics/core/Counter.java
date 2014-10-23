package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * A counter for tracking the number of some events, such as page hits.
 *
 * @author John Sanda
 */
public class Counter {

    /** An identifier for grouping related counters. */
    private String group;

    /** The counter name. */
    private String name;

    /** The counter value. */
    private long value;

    public Counter() {
    }

    public Counter(String group, String name, long value) {
        this.group = group;
        this.name = name;
        this.value = value;
    }

    /**
     * Returns an identifier for grouping related counters.
     *
     * @return an identifier for grouping related counters
     */
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Returns this {@link Counter}'s name.
     *
     * @return this {@link Counter}'s name
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns this {@link Counter}'s value.
     *
     * @return this {@link Counter}'s value
     */
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    /** @see java.lang.Object#equals(java.lang.Object) */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Counter counter = (Counter) o;

        if (!group.equals(counter.group)) return false;
        if (!name.equals(counter.name)) return false;

        return true;
    }

    /** @see java.lang.Object#hashCode() */
    @Override
    public int hashCode() {
        int result = group.hashCode();
        result = 29 * result + name.hashCode();
        return result;
    }

    /** @see java.lang.Object#toString() */
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass().getSimpleName())
            .add("group", group)
            .add("name", name)
            .add("value", value)
            .toString();
    }
}
