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
package org.hawkular.metrics.api.jaxrs.callback;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.FutureCallback;

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.MetricData;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.impl.mapper.DataPointOut;
import org.hawkular.metrics.core.impl.mapper.MetricOut;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

/**
 * @author michael
 */
public class TaggedDataCallback<T extends MetricData> implements FutureCallback<Map<MetricId, Set<T>>> {

    private AsyncResponse asyncResponse;

    public TaggedDataCallback(AsyncResponse asyncResponse) {
        this.asyncResponse = asyncResponse;
    }

    @Override
    public void onSuccess(Map<MetricId, Set<T>> taggedDataMap) {
        if (taggedDataMap.isEmpty()) {
            asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
        } else {
            Map<String, MetricOut> results = transformTaggedDataMap(taggedDataMap);
            asyncResponse.resume(Response.ok(results).type(APPLICATION_JSON_TYPE).build());
        }
    }

    @Override
    public void onFailure(Throwable t) {
        asyncResponse.resume(t);
    }

    private <T extends MetricData> Map<String, MetricOut> transformTaggedDataMap(Map<MetricId, Set<T>> taggedDataMap) {
        Map<String, MetricOut> results = new HashMap<>();
        MetricOut dataOut = null;

        for (Map.Entry<MetricId, Set<T>> setEntry : taggedDataMap.entrySet()) {
            List<DataPointOut> dataPoints = new ArrayList<>();

            for (T d : setEntry.getValue()) {
                if (dataOut == null) {
                    dataOut = new MetricOut(d.getMetric().getTenantId(), d.getMetric().getId().getName(), null);
                }

                dataPoints.add(new DataPointOut(d.getTimestamp(), getValue(d)));
            }
            dataOut.setData(dataPoints);
            results.put(setEntry.getKey().getName(), dataOut);
        }
        return results;
    }

    private <T extends MetricData> Object getValue(T metric) {
        Object value = null;

        if(metric instanceof Availability) {
            value = ((Availability) metric).getType().getText();
        } else if(metric instanceof NumericData) {
            value = ((NumericData) metric).getValue();
        }
        return value;
    }
}
