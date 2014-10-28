package org.rhq.metrics.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Objects;

/**
 * RHQ Metrics provides multi-tenancy support. This means that all data is implicitly partitioned by tenant. Tags,
 * data retention, and pre-computed aggregates are also per-tenant. Note that data retention and pre-computed aggregates
 * can be configured more narrowly, by tag and by individual metric.
 *
 * @author John Sanda
 */
public class Tenant {

    private String id;

    private List<AggregationTemplate> aggregationTemplates = new ArrayList<>();

    private RetentionSettings retentionSettings = new RetentionSettings();

    public String getId() {
        return id;
    }

    public Tenant setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * The configured {@link org.rhq.metrics.core.AggregationTemplate aggregation templates} for the tenant
     */
    public List<AggregationTemplate> getAggregationTemplates() {
        return aggregationTemplates;
    }

    public Tenant addAggregationTemplate(AggregationTemplate template) {
        aggregationTemplates.add(template);
        return this;
    }

    /**
     * The {@link org.rhq.metrics.core.RetentionSettings data retention settings} for both raw and aggregated data of
     * all metric types
     */
    public RetentionSettings getRetentionSettings() {
        return retentionSettings;
    }

    public Tenant setRetention(MetricType type, int hours) {
        retentionSettings.put(type, hours);
        return this;
    }

    public Tenant setRetention(MetricType type, Interval interval, int hours) {
        retentionSettings.put(type, interval, hours);
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tenant tenant = (Tenant) o;

        if (!aggregationTemplates.equals(tenant.aggregationTemplates)) return false;
        if (!id.equals(tenant.id)) return false;
        if (!retentionSettings.equals(tenant.retentionSettings)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + aggregationTemplates.hashCode();
        result = 31 * result + retentionSettings.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .add("id", id)
            .add("aggregationTemplates", aggregationTemplates)
            .add("retentionSettings", retentionSettings)
            .toString();
    }
}
