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
package org.hawkular.metrics.api.jaxrs;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.jboss.logging.Logger;

/**
 * This is a lock implementation built on top of an Infinispan replicated/distributed cache. If the cache is local,
 * the the methods in this class are basically no-ops. Note that the locking implementation is process-wide.
 *
 * @author jsanda
 */
@Listener
public class DistributedLock {

    private static Logger log = Logger.getLogger(DistributedLock.class);

    public static final long DEFAULT_RETRY_DELAY = 10000;

    private final AdvancedCache<String, String> locksCache;

    private final String key;

    /**
     * @param locksCache The cache used for locking. This class will probably refactored at some point so that the cache
     *                   can be directly injected.
     * @param key The name or value of the lock.
     */
    public DistributedLock(AdvancedCache<String, String> locksCache, String key) {
        this.key = key;
        this.locksCache = locksCache;
        this.locksCache.getCacheManager().addListener(this);
    }

    private boolean isLocked() {
        if (isDistributed()) {
            return getLockValue().equals(locksCache.get(key));
        }
        return false;
    }

    /**
     * Returns true if the cache is local; otherwise, return true if the lock is acquired. The lock is held indefinitely
     * until either {@link #release()} is called or the owning process goes down. If the owner goes down, the rest of
     * the cluster will clear the lock such that other cluster members can acquire it.
     */
    public boolean lock() {
        if (isDistributed()) {
            if (isLocked()) {
                return true;
            }
            return locksCache.putIfAbsent(key, getLockValue()) == null;
        }
        return true;
    }

    /**
     * Returns true if the cache is local. Returns true if this node owns the lock and is able to remove it from the
     * cache.
     */
    public boolean release() {
        if (isDistributed() && isLocked()) {
            return locksCache.remove(key, getLockValue());
        }
        return true;
    }

    /**
     * Blocks until the lock is acquired and then execute runnable. After runnable finishes, the lock is released.
     * There is a delay of {@link #DEFAULT_RETRY_DELAY} ms between successive attempts at acquiring the lock.
     */
    public void lockAndThen(Runnable runnable) {
        lockAndThen(DEFAULT_RETRY_DELAY, runnable);
    }

    /**
     * Blocks until the lock is acquired and then execute runnable. After runnable finishes, the lock is released.
     * There is a delay of retryDelay ms between successive attempts at acquiring the lock.
     */
    public void lockAndThen(long retryDelay, Runnable runnable) {
        try {
            while (!lock()) {
                log.debugf("Failed to acquire [%s] lock. Trying again in [%d] ms", key, retryDelay);
                Thread.sleep(retryDelay);
                lock();
            }
            runnable.run();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            release();
        }
    }

    @ViewChanged
    public void viewChanged(ViewChangedEvent event) {
        log.debugf("view changed: %s", event);
        List<Address> old = new ArrayList<>(event.getOldMembers());
        old.removeAll(event.getNewMembers());
        for (Address address : old) {
            locksCache.remove(key, address.toString());
        }
    }

    private boolean isDistributed() {
        return locksCache.getCacheManager().getTransport() != null;
    }

    private String getLockValue() {
        return locksCache.getCacheManager().getAddress().toString();
    }

}
