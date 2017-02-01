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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hawkular.rx.cassandra.driver.RxSession;
import org.jboss.logging.Logger;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import rx.Observable;

/**
 * <p>
 * This class implements a distributed locking service. Locks expire using Cassandra's TTL. Locks are automatically
 * renewed with a renewal frequency determined by the lock timeout (i.e., TTL). The idea is to renew locks frequently
 * enough to avoid locks accidentally expiring without placing too much extra load on Cassandra with frequent renewals.
 * Locks can be explicitly released as well.
 * </p>
 * <p>
 * Note that this is still very much a work in progress. LockManager is currently used only by the job scheduler, but
 * could be used a general purpose distributed locking service. Failure situations are not yet handled. For example,
 * suppose renewing a lock fails because the CQL query to renew it times out. We simply stop trying to renew the lock,
 * allowing it to expire. There needs to be some sort of notification mechanism to inform a lock holder when renewal
 * fails so that the client can act accordingly.
 * </p>
 * <p>
 * Lastly, Locks do not have to be automatically renewed.
 * </p>
 *
 * @author jsanda
 */
class LockManager {

    private static Logger logger = Logger.getLogger(LockManager.class);

    public static final long LOCK_RENEWAL_RATE = 10;

    private RxSession session;

    private PreparedStatement acquireLock;

    private PreparedStatement releaseLock;

    private PreparedStatement renewLock;

    private PreparedStatement getTTL;

    private ScheduledExecutorService locksExecutor;

    private Map<String, Lock> activeLocks;

    private ReentrantReadWriteLock activeLocksLock;

    public LockManager(RxSession session) {
        this.session = session;
        acquireLock = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET value = ? WHERE name = ? IF value IN (NULL, ?)");
        releaseLock = session.getSession().prepare(
                "UPDATE locks SET value = NULL WHERE name = ? IF value = ?");
        renewLock = session.getSession().prepare(
                "UPDATE locks USING TTL ? SET value = ? WHERE name = ? IF value = ?");
        getTTL = session.getSession().prepare("SELECT TTL(value) FROM locks WHERE name = ?");

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("locks-thread-pool-%d").build();
        locksExecutor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());

        activeLocks = new HashMap<>();
        activeLocksLock = new ReentrantReadWriteLock();

        locksExecutor.scheduleAtFixedRate(this::renewLocks, 0, LOCK_RENEWAL_RATE, TimeUnit.SECONDS);
    }

    /**
     * Attempt to release any locks on shutdown so that other clients can obtain those locks without having to wait
     * for them to expire.
     */
    public void shutdown() {
        try {
            locksExecutor.shutdown();
            locksExecutor.awaitTermination(5, TimeUnit.SECONDS);
            CountDownLatch latch = new CountDownLatch(1);
            activeLocksLock.writeLock().lock();
            Observable.from(activeLocks.entrySet())
                    .map(Map.Entry::getValue)
                    .flatMap(lock -> releaseLock(lock.getName(), lock.getValue())
                            .map(released -> new Lock(lock.getName(), lock.getValue(), lock.getExpiration(), lock
                                    .getRenewalRate(), !released)))
                    .subscribe(
                            lock -> {
                                if (lock.isLocked()) {
                                    logger.infof("Failed to release lock %s", lock.getName());
                                }
                            },
                            t -> {
                                logger.info("There was an error while releasing locks", t);
                                latch.countDown();
                            },
                            latch::countDown
                    );
            latch.await();
            logger.info("Shutdown complete");
        } catch (InterruptedException e) {
            logger.debug("Shutdown was interrupted. Some locks may not have been released but they will still expire.");
        }
    }

    private void renewLocks() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            logger.trace("Renewing locks");
            CountDownLatch latch = new CountDownLatch(1);
            activeLocksLock.writeLock().lock();
            Observable.from(activeLocks.entrySet())
                    .filter(entry -> (entry.getValue().getExpiration() - System.currentTimeMillis()) <=
                            entry.getValue().getRenewalRate())
                    .map(Map.Entry::getValue)
                    .doOnNext(lock -> logger.debugf("Renewing %s", lock))
                    .flatMap(this::renewLock)
                    .subscribe(
                            lock -> {
                                if (lock.isLocked()) {
                                    logger.debugf("Renewed %s", lock);
                                    activeLocks.put(lock.getName(), lock);
                                } else {
                                    logger.warnf("Failed to renew %s", lock);
                                    activeLocks.remove(lock.getName());
                                }
                            },
                            t -> {
                                logger.warn("There was an error renewing locks", t);
                                latch.countDown();
                            },
                            latch::countDown
                    );
            latch.await();
        } catch (Throwable t) {
            logger.warn("There was an error trying to renew locks", t);
        } finally {
            activeLocksLock.writeLock().unlock();
            stopwatch.stop();
            logger.tracef("Finished renewing locks in %d ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

    public Observable<Lock> acquireLock(String name, String value, int timeout, boolean autoRenew) {
        long expiration = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, TimeUnit.SECONDS);
        int renewalRate = timeout / 2;
        long expirationMicro = TimeUnit.MICROSECONDS.convert(expiration, TimeUnit.MILLISECONDS);
        return session.execute(acquireLock.bind(timeout, value, name, value)
                .setDefaultTimestamp(expirationMicro))
                .map(ResultSet::wasApplied)
                .map(locked -> {
                    Lock lock = new Lock(name, value, expiration, renewalRate, locked);
                    if (locked && autoRenew) {
                        try {
                            activeLocksLock.writeLock().lock();
                            activeLocks.put(name, lock);
                        } finally {
                            activeLocksLock.writeLock().unlock();
                        }
                    }
                    return lock;
                });
    }

    public Observable<Boolean> releaseLock(String name, String value) {
        Lock lock = null;
        try {
            activeLocksLock.writeLock().lock();
            lock = activeLocks.remove(name);
        } finally {
            activeLocksLock.writeLock().unlock();
        }
        // TODO need to handle failure situations
        // If the query fails because we no longer hold the lock then that's fine. But if the query fails say because
        // of a timeout or because C* is down, then the lock might now actually be released.
        return session.execute(releaseLock.bind(name, value)).map(ResultSet::wasApplied);
    }

    public Observable<Lock> renewLock(Lock lock) {
        long nextExpiration = System.currentTimeMillis() + (lock.getRenewalRate() * 1000);
        long nextExpirationMicro = TimeUnit.MICROSECONDS.convert(lock.getExpiration(), TimeUnit.MILLISECONDS);

        logger.debugf("Renewing %s with TTL of %d", lock.getName(), lock.getRenewalRate());

        return session.execute(acquireLock.bind(lock.getRenewalRate(), lock.getValue(), lock.getName(),
                lock.getValue())
                .setDefaultTimestamp(nextExpirationMicro))
                .map(ResultSet::wasApplied)
                .map(locked -> new Lock(lock.getName(), lock.getValue(), nextExpiration, lock.getRenewalRate(),
                        locked));
    }

}
