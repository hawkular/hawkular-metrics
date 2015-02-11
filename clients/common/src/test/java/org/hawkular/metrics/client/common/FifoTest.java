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


import java.util.Collection;
import java.util.List;

import org.junit.Test;

/**
 *
 * @author Heiko W. Rupp
 */
public class FifoTest {

    @Test
    public void testEmptyFifo() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 15);
        assert fifo.size()==0;

        assert fifo.isEmpty();

        assert fifo.poll()==null;

    }

    @Test
    public void testFifoAddOne() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 15);

        SingleMetric metric = new SingleMetric("foo",12345L,42.0d);
        fifo.offer(metric);

        assert fifo.size()==1 : "Size should be 1 but was " + fifo.size();
        assert !fifo.isEmpty() : "Fifo should not be empty but it was";

        SingleMetric pollResult = fifo.poll();
        assert pollResult != null : "Got nothing back from the fifo";
        assert pollResult.equals(metric);

    }

    @Test
    public void testOverrun() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 2);
        SingleMetric metric;

        for (int i = 1; i <= 3 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        assert fifo.size()==2 : "Fifo size should be 2, but was " + fifo.size();

        // element "1" should have been pushed out, so 2 and 3 should be there

        SingleMetric result = fifo.poll();
        assert result != null;
        assert result.getTimestamp()==2 : "Expected ts =2, but it was " + result.getTimestamp();

        result = fifo.poll();
        assert result != null;
        assert result.getTimestamp()==3 : "Expected ts =3, but it was " + result.getTimestamp();

        // we have retrieved all elements

        assert fifo.isEmpty();

    }

    @Test
    public void testFifoPeek() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 15);

        SingleMetric metric = new SingleMetric("foo",12345L,42.0d);
        fifo.offer(metric);

        assert fifo.size()==1 : "Size should be 1 but was " + fifo.size();
        assert !fifo.isEmpty() : "Fifo should not be empty but it was";

        SingleMetric peekResult = fifo.peek();
        assert peekResult != null : "Got nothing back from the fifo";
        assert peekResult.equals(metric);

        assert fifo.size()==1 : "Size should be 1 but was " + fifo.size();

        fifo = new BoundMetricFifo(10, 3);
        peekResult = fifo.peek();
        assert peekResult == null;
    }

    @Test
    public void testList() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 2);
        SingleMetric metric;

        for (int i = 1; i <= 3 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        assert fifo.size()==2 : "Fifo size should be 2, but was " + fifo.size();

        List<SingleMetric> metrics = fifo.getList();
        assert metrics.size()==2 : "Expected 2 items in list, but got " + metrics.size();

    }

    @Test
    public void testCleanoutOne1() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        SingleMetric metric = null;

        for (int i = 1; i <= 3 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        assert fifo.size()==3 : "Fifo size should be 3, but was " + fifo.size();

        fifo.cleanout(metric); // clean out item 3

        assert fifo.size()==2 : "Fifo size should be 2 but was " + fifo.size();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==1 : "Timestamp should be 1 but was " + metric.getTimestamp();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==2: "Timestamp should be 2 but was " + metric.getTimestamp();

    }

    @Test
    public void testCleanoutOne2() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        SingleMetric metric;

        SingleMetric metric1 = new SingleMetric( "1", 1, 1.0d);
        SingleMetric metric2 = new SingleMetric( "2", 2, 2.0d);
        SingleMetric metric3 = new SingleMetric( "3", 3, 3.0d);
        fifo.offer(metric1);
        fifo.offer(metric2);
        fifo.offer(metric3);

        assert fifo.size()==3 : "Fifo size should be 3, but was " + fifo.size();

        fifo.cleanout(metric2); // clean out item 2

        assert fifo.size()==2 : "Fifo size should be 2 but was " + fifo.size();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==1 : "Timestamp should be 3 but was " + metric.getTimestamp();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==3: "Timestamp should be 3 but was " + metric.getTimestamp();

    }


    @Test
    public void testCleanoutList1() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 10);
        SingleMetric metric;

        for (int i = 1; i <= 3 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        assert fifo.size()==3 : "Fifo size should be 3, but was " + fifo.size();
        List<SingleMetric> metrics = fifo.getList();

        for (int i = 4; i <= 6 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        fifo.cleanout(metrics);

        assert fifo.size()==3 : "Fifo size should be 3 but was " + fifo.size();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==4;


    }

    @Test
    public void testCleanoutList2() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        SingleMetric metric;

        for (int i = 1; i <= 3 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        assert fifo.size()==3 : "Fifo size should be 3, but was " + fifo.size();
        List<SingleMetric> metrics = fifo.getList();


        for (int i = 4; i <= 6 ; i++) {
            metric = new SingleMetric(""+i,(long)i,(double)i);
            fifo.offer(metric);
        }

        fifo.cleanout(metrics);

        assert fifo.size()==3 : "Fifo size should be 3 but was " + fifo.size();

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==4;

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==5;

        metric = fifo.poll();
        assert metric != null;
        assert metric.getTimestamp()==6;

    }

    @Test
    public void testCleanoutNonExistent() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);

        SingleMetric metric = new SingleMetric( "1", 1, 1.0d);

        boolean removed = fifo.cleanout(metric);

        assert !removed;

    }

    @Test
    public void testCleanoutNull() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);

        assert !fifo.cleanout((Collection)null);
        assert !fifo.cleanout((SingleMetric)null);

    }
}
