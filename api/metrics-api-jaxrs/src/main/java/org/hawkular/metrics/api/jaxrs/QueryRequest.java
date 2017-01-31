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
package org.hawkular.metrics.api.jaxrs;

import java.util.List;

/**
 * @author jsanda
 */
public class QueryRequest {

    private List<String> ids;

    private String start;

    private String end;

    private Boolean fromEarliest;

    private Integer limit;

    private String order;

    private String tags;

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public Boolean getFromEarliest() {
        return fromEarliest;
    }

    public void setFromEarliest(Boolean fromEarliest) {
        this.fromEarliest = fromEarliest;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    @Override public String toString() {
        return "QueryRequest{" +
                "ids=" + ids +
                ", start=" + start +
                ", end=" + end +
                ", fromEarliest=" + fromEarliest +
                ", limit=" + limit +
                ", order=" + order +
                ", tags=" + tags +
                '}';
    }
}
