/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.impl;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * Represents a lock stored in the locks table in Cassandra. Locks expire using Cassandra's TTL. See
 * {@link LockManager} for more details.
 *
 * @author jsanda
 */
public class Lock {

    private final String name;

    private final String value;

    /**
     * This should be considered an approximation since we cannot guarantee clock synchronization, and the value is
     * a timestamp with millisecond precision.
     */
    private final long expirationTime;

    /**
     * This is a duration in seconds that serves two purposes. First, it used for the lock's TTL. Secondly, it used
     * threshold to determine when to renew the lock. If the amount of time left before the lock expires is less than
     * the renewal rate, then an atempt will be made to renew it.
     */
    private final int renewalRate;

    private final boolean locked;

    public Lock(String name, String value, long expirationTime, int renewalRate) {
        this.name = name;
        this.value = value;
        this.expirationTime = expirationTime;
        this.renewalRate = renewalRate;
        this.locked = false;
    }

    public Lock(String name, String value, long expirationTime, int renewalRate, boolean locked) {
        this.name = name;
        this.value = value;
        this.expirationTime = expirationTime;
        this.renewalRate = renewalRate;
        this.locked = locked;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public long getExpiration() {
        return expirationTime;
    }

    public int getRenewalRate() {
        return renewalRate;
    }

    public boolean isLocked() {
        return locked;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return expirationTime == lock.expirationTime &&
                renewalRate == lock.renewalRate &&
                locked == lock.locked &&
                Objects.equals(name, lock.name) &&
                Objects.equals(value, lock.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, expirationTime, renewalRate, locked);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .add("expirationTime", expirationTime)
                .add("renewalRate", renewalRate)
                .add("locked", locked)
                .toString();
    }

}
