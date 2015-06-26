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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A FIFO buffer with fixed capacity. When capacity is reached, older elements are discarded. Metric buffers are not
 * thread-safe. Insertion of null elements is silently ignored.
 * <p>
 * This class is particularly useful when you want to:
 * <ul>
 * <li>buffer metrics collected before sending to the backend</li>
 * <li>retrieve metrics from the buffer in batches for sending</li>
 * <li>re-insert batches in the buffer if sending failed (as much as capacity permits)</li>
 * </ul>
 *
 * @author Thomas Segismont
 */
public final class MetricBuffer {
    private final int capacity;
    private final Deque<SingleMetric> buffer;

    /**
     * Creates a new buffer with fixed capacity.
     *
     * @param capacity the maximum number of elements in the buffer
     */
    public MetricBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity: " + capacity);
        }
        this.capacity = capacity;
        buffer = new ArrayDeque<>(capacity);
    }

    /**
     * @return the number of elements in this buffer
     */
    public int size() {
        return buffer.size();
    }

    /**
     * @return the number of elements which can be inserted without discarding older elements
     */
    public int remainingCapacity() {
        return capacity - buffer.size();
    }

    /**
     * Inserts a new element in this buffer. If {@link #remainingCapacity()} is zero, the oldest element is discarded.
     *
     * @param metric silently ignored if null
     */
    public void insert(SingleMetric metric) {
        if (metric == null) {
            return;
        }
        if (remainingCapacity() == 0) {
            buffer.removeLast();
        }
        buffer.addFirst(metric);
    }

    /**
     * Retrieves and removes the oldest elements in this buffer.
     *
     * @param batchSize the desired number of elements to return
     *
     * @return a list of N elements, in insertion order, where N is the minimum of {@code batchSize} and buffer size
     *
     * @throws IllegalArgumentException if {@code batchSize < 0}
     */
    public List<SingleMetric> remove(int batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize: " + batchSize);
        }
        int count = Math.min(buffer.size(), batchSize);
        List<SingleMetric> metrics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            metrics.add(buffer.removeLast());
        }
        return metrics;
    }

    /**
     * Re-inserts a batch previously removed with {@link #remove(int)}. Elements are re-inserted as much as
     * capacity permits. The elements are expected to be sorted in their order of insertion in the buffer before they
     * were removed.
     * <p>
     * <strong>Example</strong>
     * <p>
     * Consider a buffer with a capacity of 5 elements. The buffer is filled with five elements:
     * <pre>buffer.insert(metric1); ... ; buffer.insert(metric5);</pre>
     * The buffer looks like:
     * <pre>metric5 &gt;&gt; metric4 &gt;&gt; metric3 &gt;&gt; metric2 &gt;&gt; metric1</pre>
     * A batch of two elements is removed:
     * <pre>List batch = buffer.remove(2);</pre>
     * The buffer looks like:
     * <pre>metric5 &gt;&gt; metric4 &gt;&gt; metric3</pre>
     * And the {@code batch} list:
     * <pre>metric1 &lt;&lt; metric2</pre>
     * Now a new element is inserted:
     * <pre>buffer.insert(metric6);</pre>
     * The buffers looks like:
     * <pre>metric6 &gt;&gt; metric5 &gt;&gt; metric4 &gt;&gt; metric3</pre>
     * But the batch previously removed needs to be re-inserted, because sending failed.
     * <pre>buffer.reInsert(batch);</pre>
     * As there's room for a single element, after re-insertion the buffer will look like:
     * <pre>metric6 &gt;&gt; metric5 &gt;&gt; metric4 &gt;&gt; metric3 &gt;&gt; metric2</pre>
     *
     * @param metrics silently ignored if null or empty; null elements in the list are also silently ignored
     */
    public void reInsert(List<SingleMetric> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return;
        }
        int remainingCapacity = remainingCapacity();
        if (remainingCapacity < metrics.size()) {
            metrics = metrics.subList(metrics.size() - remainingCapacity, metrics.size());
        }
        for (int i = metrics.size(); i > 0; i--) {
            SingleMetric metric = metrics.get(i - 1);
            if (metric != null) {
                buffer.addLast(metric);
            }
        }
    }
}