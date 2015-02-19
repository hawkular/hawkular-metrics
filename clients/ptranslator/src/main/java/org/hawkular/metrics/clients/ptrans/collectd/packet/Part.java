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
package org.hawkular.metrics.clients.ptrans.collectd.packet;

/**
 * Abstract structure representing a <a href="https://collectd.org/wiki/index
 * .php/Binary_protocol#Protocol_structure">collectd
 * datagram part</a>.
 * <p>
 * A Part is composed of a {@link PartType} and a value.
 *
 * @author Thomas Segismont
 * @see NumericPart
 * @see StringPart
 * @see ValuePart
 */
public abstract class Part<T> {
    private final PartType partType;
    private final T value;

    protected Part(PartType partType, T value) {
        this.partType = partType;
        this.value = value;
    }

    public final PartType getPartType() {
        return partType;
    }

    public final T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Part[" + "partType=" + partType + ", value=" + value + ']';
    }
}
