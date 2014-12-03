package org.rhq.metrics.restServlet;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * @author John Sanda
 */
public class DataPointOut {

    private long timestamp;
    private Object value;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private Set<String> tags = new HashSet<>();

    public DataPointOut() {
    }

    public DataPointOut(long timestamp, Object value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataPointOut(long timestamp, Object value, Set<String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

}
