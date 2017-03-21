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
package org.hawkular.metrics.api.jaxrs.handler.template;

import java.util.List;
import java.util.Map;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.QueryRequest;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;

/**
 * @author Stefan Negrea
 *
 */
public interface IMetricsHandler<T> {

    //Metric
    void getMetrics(AsyncResponse asyncResponse, String tags, Boolean fetchTimestamps);

    void createMetric(AsyncResponse asyncResponse, Metric<T> metric, Boolean overwrite, UriInfo uriInfo);

    void getMetric(AsyncResponse asyncResponse, String id);

    void deleteMetric(AsyncResponse asyncResponse, String id);

    //Tags
    void getTags(AsyncResponse asyncResponse, Tags tags);

    void getMetricTags(AsyncResponse asyncResponse, String id);

    void updateMetricTags(AsyncResponse asyncResponse, String id, Map<String, String> tags);

    void deleteMetricTags(AsyncResponse asyncResponse, String id, TagNames tags);

    //Data
    void addData(AsyncResponse asyncResponse, List<Metric<T>> metrics);

    void getData(AsyncResponse asyncResponse, QueryRequest query);

    void addMetricData(AsyncResponse asyncResponse, String id, List<DataPoint<T>> data);
}
