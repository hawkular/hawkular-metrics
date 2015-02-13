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
 * Collectd <a href="https://collectd.org/wiki/index.php/Binary_protocol#Numeric_parts">Numeric Part</a>.
 *
 * @author Thomas Segismont
 */
public final class NumericPart extends Part<Long> {
    public NumericPart(PartType partType, Long value) {
        super(partType, value);
    }
}
