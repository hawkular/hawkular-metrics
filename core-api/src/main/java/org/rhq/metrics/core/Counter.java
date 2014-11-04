package org.rhq.metrics.core;

import com.google.common.base.Objects;

/**
 * A counter for tracking the number of some events, such as page hits.
 *
 * @author John Sanda
 */
public class Counter {

    private String tenantId;

    /** An identifier for grouping related counters. */
    private String group;

    /** The counter name. */
    private String name;

    /** The counter value. */
    private long value;

    public Counter(String tenantId, String group, String name, long value) {
        this.tenantId = tenantId;
        this.group = group;
        this.name = name;
        this.value = value;
    }

    /**
     * @return The id of the owning tenant
     */
    public String getTenantId() {
        return tenantId;
    }

    /**
     * @return An identifier for grouping related counters
     */
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * @return The counter name
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return The counter value
     */
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Counter counter = (Counter) o;

        if (value != counter.value) return false;
        if (!group.equals(counter.group)) return false;
        if (!name.equals(counter.name)) return false;
        if (!tenantId.equals(counter.tenantId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId.hashCode();
        result = 31 * result + group.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (int) (value ^ (value >>> 32));
        return result;
    }

    /** @see java.lang.Object#toString() */
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass().getSimpleName())
            .add("tenantId", tenantId)
            .add("group", group)
            .add("name", name)
            .add("value", value)
            .toString();
    }
}
