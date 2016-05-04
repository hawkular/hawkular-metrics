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
package org.hawkular.metrics.model;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModel;

/**
 * @author John Sanda
 */
@ApiModel
public class TagRequest {
    private final Map<String, String> tags;
    private final Long start;
    private final Long end;
    private final Long timestamp;

    @JsonCreator(mode = Mode.PROPERTIES)
    public TagRequest(
            @JsonProperty("tags")
            Map<String, String> tags,
            @JsonProperty("start")
            Long start,
            @JsonProperty("end")
            Long end,
            @JsonProperty("timestamp")
            Long timestamp
    ) {
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
        this.start = start;
        this.end = end;
        this.timestamp = timestamp;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("tags", tags)
                .add("start", start)
                .add("end", end)
                .add("timestamp", timestamp)
                .omitNullValues()
                .toString();
    }
}
