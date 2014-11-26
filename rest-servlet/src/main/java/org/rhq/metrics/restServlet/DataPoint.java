package org.rhq.metrics.restServlet;

import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.wordnik.swagger.annotations.ApiClass;
import com.wordnik.swagger.annotations.ApiProperty;

/**
 * One single data point
 * @author Heiko W. Rupp
 */
@ApiClass("A data point for collections where each data point has the same id.")
@XmlRootElement
public class DataPoint {

    private long timestamp;
    private Object value;

    @JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
    private Set<String> tags = new HashSet<>();

    public DataPoint() {
    }

    public DataPoint(long timestamp, Object value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public DataPoint(long timestamp, Object value, Set<String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    @ApiProperty("Time when the value was obtained in milliseconds since epoch")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiProperty("The value of this data point")
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
