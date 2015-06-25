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

import static java.util.stream.Collectors.toList;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * @author Thomas Segismont
 */
public class MetricBufferTest {

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private final MetricBuffer buffer = new MetricBuffer(10);

    @Test
    public void shouldNotFailOnNullInsert() {
        buffer.insert(null);
    }

    @Test
    public void shouldRemoveInInsertionOrder() {
        List<SingleMetric> expected = IntStream.range(0, 5).mapToObj(i -> createMetric()).collect(toList());
        expected.forEach(buffer::insert);
        assertEquals(expected, buffer.remove(buffer.size()));
    }

    @Test
    public void shouldDiscardOlderElementsWhenFull() {
        List<SingleMetric> inserted = IntStream.range(0, 15).mapToObj(i -> createMetric()).collect(toList());
        inserted.forEach(buffer::insert);
        List<SingleMetric> expected = inserted.subList(5, 15);
        assertEquals(expected, buffer.remove(buffer.size()));
    }

    @Test
    public void shouldReInsertAtTheEnd() {
        List<SingleMetric> expected = IntStream.range(0, 5).mapToObj(i -> createMetric()).collect(toList());
        expected.forEach(buffer::insert);

        List<SingleMetric> removed = buffer.remove(expected.size() - 1);
        buffer.reInsert(removed);

        assertEquals(expected, buffer.remove(buffer.size()));
    }

    @Test
    public void shouldNotFailOnNullListReInsert() {
        buffer.reInsert(null);
    }

    @Test
    public void shouldNotReInsertNullElement() {
        List<SingleMetric> expected = IntStream.range(0, 5).mapToObj(i -> createMetric()).collect(toList());
        expected.forEach(buffer::insert);

        List<SingleMetric> removed = buffer.remove(expected.size());
        List<SingleMetric> reinserted = IntStream.range(0, 2 * removed.size())
                .mapToObj(i -> i % 2 == 0 ? removed.get(i / 2) : null)
                .collect(toList());
        buffer.reInsert(reinserted);

        assertEquals(expected, buffer.remove(buffer.size()));
    }

    @Test
    public void shouldNotReInsertMoreThanRemainingCapacity() {
        List<SingleMetric> firstInserted = IntStream.range(0, 10).mapToObj(i -> createMetric()).collect(toList());
        firstInserted.forEach(buffer::insert);
        // Buffer now looks like: 10 >> ... >> 1

        List<SingleMetric> removed = buffer.remove(5);
        // Buffer view: 10 >> ... >> 6

        List<SingleMetric> secondInserted = IntStream.range(0, 3).mapToObj(i -> createMetric()).collect(toList());
        secondInserted.forEach(buffer::insert);
        // Buffer view: 13 >> ... >> 6

        buffer.reInsert(removed);
        // Buffer view: 13 >> ... >> 4

        List<SingleMetric> expected = new ArrayList<>();
        expected.addAll(firstInserted.subList(3, 10));
        expected.addAll(secondInserted);

        assertEquals(expected, buffer.remove(buffer.size()));
    }

    private SingleMetric createMetric() {
        int id = idGenerator.incrementAndGet();
        return new SingleMetric(String.valueOf(id), id, (double) id);
    }
}