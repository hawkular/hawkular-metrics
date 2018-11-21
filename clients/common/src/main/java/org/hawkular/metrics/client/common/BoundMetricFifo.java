/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.client.common;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * A Fifo with a fixed maximum size. If the maximum size is reached, old
 * elements are pushed out to make room for the new arrivals.
 *
 * Access is internally synchronized.
 *
 * There are special methods #getList and #cleanout that can be used to
 * retrieve a list of items for further processing so that the fifo can still
 * be offered new items. If the list processing was successful, #cleanout can
 * be used to remove those items from the fifo.
 * @author Heiko W. Rupp
 */
public class BoundMetricFifo extends AbstractQueue<SingleMetric> implements Queue<SingleMetric>{

    private final int maxSize;
    Deque<SingleMetric> contents;
    final Object mutex;

    /**
     * Create a new Fifo with an initial internal capacity of initialSize and a
     * maximum capacity of maxSize.
     * @param initialSize Initial internal capacity.
     * @param maxSize Maximum capacity of the Fifo.
     */
    public BoundMetricFifo(int initialSize, int maxSize) {
        if (initialSize > maxSize) {
            initialSize =  maxSize;
        }
        contents = new ArrayDeque<>(initialSize);
        this.maxSize = maxSize;
        mutex = this;
    }

    @Override
    public Iterator<SingleMetric> iterator() {
        synchronized (mutex) {
            return contents.iterator();
        }
    }

    @Override
    public int size() {
        synchronized (mutex) {
            return contents.size();
        }
    }

    /**
     * Add an object to the Fifo. If the Fifo is full,
     * then the oldest item is removed first.
     * @param singleMetric Metric to add
     * @return true if adding was successful
     */
    @Override
    public boolean offer(SingleMetric singleMetric) {

        synchronized (mutex) {
            if (contents.size()==maxSize) {
                contents.removeLast();
            }
            contents.offerFirst(singleMetric);
        }

        return true;
    }

    @Override
    public SingleMetric poll() {
        synchronized (mutex) {
            if (contents.size()==0) {
                return null;
            }
            else {
                return contents.removeLast();
            }
        }
    }

    @Override
    public SingleMetric peek() {
        synchronized (mutex) {
            return contents.peekLast();
        }
    }

    /**
     * Remove the single passed metric from the fifo
     * @param metric Metric to remove
     * @return true if it was removed, false otherwise (e.g. if the metric was not in the fifo)
     */
    public boolean cleanout(SingleMetric metric) {
        if (metric==null) {
            return false;
        }

        synchronized (mutex) {
            return contents.remove(metric);
        }
    }

    /**
     * Remove the collection of passed metrics from the fifo.
     * @param metrics Metrics to remove
     * @return true if all Metrics were removed, false otherwise (e.g. if one metric was not in the fifo)
     */
    public boolean cleanout(Collection<SingleMetric> metrics) {

        if (metrics==null || metrics.isEmpty()) {
            return false;
        }

        boolean removed = true;

        synchronized (mutex) {
            for (SingleMetric metric : metrics) {
                removed &= contents.remove(metric);
            }
        }
        return removed;
    }

    /**
     * Retrieve a copy of the current items in the fifo. The original content of the fifo is not changed.
     * @return A copy of the items in the fifo.
     */
    public List<SingleMetric> getList() {
        synchronized (mutex) {
            return new ArrayList<>(contents);
        }
    }
}
