package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public class TenantDoesNotExistException extends RuntimeException {

    private String tenantId;

    public TenantDoesNotExistException(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }
}
