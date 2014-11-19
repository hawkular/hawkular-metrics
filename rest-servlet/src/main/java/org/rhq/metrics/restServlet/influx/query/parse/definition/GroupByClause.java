package org.rhq.metrics.restServlet.influx.query.parse.definition;

/**
 * @author Thomas Segismont
 */
public class GroupByClause {
    private final String bucketType;
    private final int bucketSize;
    private final InfluxTimeUnit bucketSizeUnit;

    public GroupByClause(String bucketType, int bucketSize, InfluxTimeUnit bucketSizeUnit) {
        this.bucketType = bucketType;
        this.bucketSize = bucketSize;
        this.bucketSizeUnit = bucketSizeUnit;
    }

    public String getBucketType() {
        return bucketType;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public InfluxTimeUnit getBucketSizeUnit() {
        return bucketSizeUnit;
    }
}
