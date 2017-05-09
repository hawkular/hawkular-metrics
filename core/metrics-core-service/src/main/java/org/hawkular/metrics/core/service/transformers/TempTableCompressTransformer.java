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
package org.hawkular.metrics.core.service.transformers;

import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.hawkular.metrics.core.service.compress.CompressedPointContainer;
import org.hawkular.metrics.core.service.compress.CompressorHeader;
import org.hawkular.metrics.core.service.compress.TagsSerializer;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;

import com.datastax.driver.core.Row;

import fi.iki.yak.ts.compression.gorilla.ByteBufferBitOutput;
import fi.iki.yak.ts.compression.gorilla.Compressor;
import rx.Observable;

/**
 * DataPointCompressor for the type 01 (plain Gorilla)
 *
 * @author Michael Burman
 */
public class TempTableCompressTransformer<T> implements Observable.Transformer<Row, CompressedPointContainer> {

    private ByteBufferBitOutput out;
    private long sliceTimestamp;
    private MetricType<T> metricType;
    private Compressor compressor;
    private TagsSerializer tagsSerializer;

    public TempTableCompressTransformer(MetricId<T> key, long timeslice) {
        out = new ByteBufferBitOutput();
        this.metricType = key.getType();

        // Write the appropriate header, at first we're stuck to Gorilla only
        byte gorillaHeader = CompressorHeader.getHeader(CompressorHeader.Compressor.GORILLA, EnumSet.noneOf
                (CompressorHeader.GorillaSettings.class));
        out.getByteBuffer().put(gorillaHeader);

        this.sliceTimestamp = timeslice;
        this.compressor = new Compressor(timeslice, out);
        this.tagsSerializer = new TagsSerializer(timeslice);
    }

    @Override
    public Observable<CompressedPointContainer> call(Observable<Row> dataRow) {
        return dataRow.collect(CompressedPointContainer::new,
                (container, r) -> {
                    // "SELECT tenant_id, type, metric, time, n_value, availability, l_value, tags FROM %s " +
                    long timestamp = r.getTimestamp(3).getTime(); // Check validity
                    switch(metricType.getCode()) {
                        case 0: // GAUGE
                            compressor.addValue(timestamp, r.getDouble(4));
                            break;
                        case 1: // AVAILABILITY
                            // TODO Update to newer Gorilla-TSC to fix these - no point storing as FP
                            compressor.addValue(timestamp, ((Byte) r.getByte(4)).doubleValue());
                            break;
                        case 2: // COUNTER
                            // TODO Update to newer Gorilla-TSC to fix these - no point storing as FP
                            compressor.addValue(timestamp, ((Long) r.getLong(4)).doubleValue());
                            break;
                        default:
                            // Not supported yet
                            throw new RuntimeException("Metric of type " + metricType.getText() + " is not supported " +
                                    "in compression");
                    }

                    // TODO Fix Tags storage!
//                    if(d.getTags() != null && !d.getTags().isEmpty()) {
//                        tagsSerializer.addDataPointTags(d.getTimestamp(), d.getTags());
//                    }
                })
                .doOnNext(cpc -> {
                    compressor.close();
                    // Update to use long words
                    ByteBuffer valueBuffer = (ByteBuffer) out.getByteBuffer().flip();
                    ByteBuffer tagsBuffer = (ByteBuffer) tagsSerializer.getByteBuffer().flip();
                    cpc.setValueBuffer(valueBuffer);
                    if(tagsBuffer.limit() > 1) {
                        // Exclude header
                        cpc.setTagsBuffer(tagsBuffer);
                    }
                });
    }
}
