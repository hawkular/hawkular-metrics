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
package org.hawkular.metrics.core.service.compress;

import java.nio.ByteBuffer;

/**
 * Holds references to the row's byteBuffers. If only value is set, then that should include all the necessary
 * information for both tags as well as timestamps.
 *
 * @author Michael Burman
 */
public class CompressedPointContainer {

    private ByteBuffer valueBuffer = null;
    private ByteBuffer timestampBuffer = null;
    private ByteBuffer tagsBuffer = null;

    public ByteBuffer getValueBuffer() {
        return valueBuffer;
    }

    public void setValueBuffer(ByteBuffer valueBuffer) {
        this.valueBuffer = valueBuffer;
    }

    public ByteBuffer getTimestampBuffer() {
        return timestampBuffer;
    }

    public void setTimestampBuffer(ByteBuffer timestampBuffer) {
        this.timestampBuffer = timestampBuffer;
    }

    public ByteBuffer getTagsBuffer() {
        return tagsBuffer;
    }

    public void setTagsBuffer(ByteBuffer tagsBuffer) {
        this.tagsBuffer = tagsBuffer;
    }
}
