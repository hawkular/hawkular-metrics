package org.rhq.metrics.core;

/**
 * @author John Sanda
 */
public class TenantAlreadyExistsException extends RuntimeException {

    private String tenantId;

    public TenantAlreadyExistsException(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }

}
