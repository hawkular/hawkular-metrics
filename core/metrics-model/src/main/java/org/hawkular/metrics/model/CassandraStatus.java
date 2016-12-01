/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.model;

import java.util.Objects;

import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jsanda
 */
public class CassandraStatus {

    private String address;

    private String status;

    public CassandraStatus() {
    }

    public CassandraStatus(String address, String status) {
        this.address = address;
        this.status = status;
    }

    @ApiModelProperty("The address on which the Cassandra node listens for client requests")
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @ApiModelProperty("Indicates whether the node is up or down")
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraStatus that = (CassandraStatus) o;
        return Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this).add("address", address).add("status", status).toString();
    }
}
