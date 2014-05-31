package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public class Counter {

    private String group;

    private String name;

    private long value;

    public Counter() {
    }

    public Counter(String group, String name, long value) {
        this.group = group;
        this.name = name;
        this.value = value;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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

        if (!group.equals(counter.group)) return false;
        if (!name.equals(counter.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = group.hashCode();
        result = 29 * result + name.hashCode();
        return result;
    }
}
