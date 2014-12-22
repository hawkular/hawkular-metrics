package org.rhq.metrics.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public abstract class Metric<T extends MetricData> {

    public static final long DPART = 0;

    private String tenantId;

    private MetricId id;

    private Map<String, String> Metadata = new HashMap<>();

    // When we implement date partitioning, dpart will have to be determined based on the
    // start and end params of queries. And it is possible the the date range spans
    // multiple date partitions.
    private long dpart = DPART;

    private List<T> data = new ArrayList<>();

    private Integer dataRetention;

    protected Metric(String tenantId, MetricId id) {
        this.tenantId = tenantId;
        this.id = id;
    }

    protected Metric(String tenantId, MetricId id, Map<String, String> Metadata) {
        this.tenantId = tenantId;
        this.id = id;
        this.Metadata = Metadata;
    }

    protected Metric(String tenantId, MetricId id, Map<String, String> Metadata, Integer dataRetention) {
        this.tenantId = tenantId;
        this.id = id;
        this.Metadata = Metadata;
        // If the data_retention column is not set, the driver returns zero instead of null.
        // We are (at least for now) using null to indicate that the metric does not have
        // the data retention set.
        if (dataRetention == null || dataRetention == 0) {
            this.dataRetention = null;
        } else {
            this.dataRetention = dataRetention;
        }
    }

    public abstract MetricType getType();

    public String getTenantId() {
        return tenantId;
    }

    public Metric setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    public MetricId getId() {
        return id;
    }

    public void setId(MetricId id) {
        this.id = id;
    }

    public long getDpart() {
        return dpart;
    }

    public void setDpart(long dpart) {
        this.dpart = dpart;
    }

    /**
     * A set of key/value pairs that are shared by all data points for the metric. A good example is units like KB/sec.
     */
    public Map<String, String> getMetadata() {
        return Metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.Metadata = metadata;
    }

    /**
     * Stores an attribute which will be shared by all data points for the metric when it is persisted. If an attribute
     * with the same name already exists, it will be overwritten.
     *
     * @param name The attribute name.
     * @param value The attribute value
     */
    public void setAttribute(String name, String value) {
        Metadata.put(name, value);
    }

    public List<T> getData() {
        return data;
    }

    public void addData(T d) {
        this.data.add(d);
        d.setMetric(this);
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Metric metric = (Metric) o;

        if (dpart != metric.dpart) return false;
        if (Metadata != null ? !Metadata.equals(metric.Metadata) : metric.Metadata != null) return false;
        if (dataRetention != null ? !dataRetention.equals(metric.dataRetention) : metric.dataRetention != null)
            return false;
        if (!id.equals(metric.id)) return false;
        if (!tenantId.equals(metric.tenantId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + (Metadata != null ? Metadata.hashCode() : 0);
        result = 31 * result + (int) (dpart ^ (dpart >>> 32));
        result = 31 * result + (dataRetention != null ? dataRetention.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("id", id)
            .add("metadata", Metadata)
            .add("dpart", dpart)
            .add("dataRetention", dataRetention)
            .toString();
    }
}
