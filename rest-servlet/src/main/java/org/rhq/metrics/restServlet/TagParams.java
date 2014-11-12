package org.rhq.metrics.restServlet;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class TagParams {

    private String tenantId;

    private Set<String> tags = new HashSet<>();

    private String metric;

    private String interval;

    private String metricType;

    private Long start;

    private Long end;

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }

    public String getMetricType() {
        return metricType;
    }

    public void setMetricType(String metricType) {
        this.metricType = metricType;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("metric", metric)
            .add("interval", interval)
            .add("metricType", metricType)
            .add("tags", tags)
            .add("start", start)
            .add("end", end)
            .toString();
    }
}
