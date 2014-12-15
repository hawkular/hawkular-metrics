package org.rhq.metrics.restServlet;

import java.util.HashMap;
import java.util.Map;

/**
 * @author John Sanda
 */
public class MetricParams {

    private String name;

    private Map<String, String> metadata = new HashMap<>();

    private Integer dataRetention;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }
}
