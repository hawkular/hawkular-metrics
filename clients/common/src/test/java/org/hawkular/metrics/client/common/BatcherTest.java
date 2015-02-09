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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * Test the batcher functionality
 * @author Heiko W. Rupp
 */
public class BatcherTest {

    @Test
    public void testNoItemsList() throws Exception {

        String json = Batcher.metricListToJson(Collections.EMPTY_LIST);

        assert json != null;
        assert json.equals("[]");
    }

    @Test
    public void testNoItemsFifo() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        assert  fifo.isEmpty();

        String json = Batcher.metricListToJson(fifo);

        assert json != null;
        assert json.equals("[]");
    }

    @Test
    public void testOneItemList() throws Exception {

        List<SingleMetric> list = new ArrayList<>(1);
        SingleMetric metric = new SingleMetric("1",1L,1d);
        list.add(metric);
        String json = Batcher.metricListToJson(list);

        assert json != null;
        assert json.equals("["+metric.toJson()+"]");
    }

    @Test
    public void testOneItemFifo() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        SingleMetric metric = new SingleMetric("1",1L,1d);
        fifo.add(metric);
        String json = Batcher.metricListToJson(fifo);

        assert json != null;
        assert json.equals("["+metric.toJson()+"]");
    }

    @Test
    public void testTwoItemsList() throws Exception {

        List<SingleMetric> list = new ArrayList<>(1);
        SingleMetric metric = new SingleMetric("1",1L,1d);
        list.add(metric);
        SingleMetric metric2 = new SingleMetric("1",1L,1d);
        list.add(metric2);
        String json = Batcher.metricListToJson(list);

        assert json != null;
        assert json.equals("["+metric.toJson()+","+metric2.toJson()+"]");
    }

    @Test
    public void testTwoItemsFifo() throws Exception {

        BoundMetricFifo fifo = new BoundMetricFifo(10, 5);
        SingleMetric metric = new SingleMetric("1",1L,1d);
        fifo.add(metric);
        SingleMetric metric2 = new SingleMetric("1",1L,1d);
        fifo.add(metric2);
        String json = Batcher.metricListToJson(fifo);

        assert json != null;
        assert json.equals("["+metric.toJson()+","+metric2.toJson()+"]");
    }

}
