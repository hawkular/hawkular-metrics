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
import java.nio.LongBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hawkular.metrics.core.service.compress.CompressedPointContainer;
import org.hawkular.metrics.core.service.compress.CompressorHeader;
import org.hawkular.metrics.core.service.compress.TagsSerializer;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.MetricType;

import com.datastax.driver.core.Row;

import fi.iki.yak.ts.compression.gorilla.GorillaCompressor;
import fi.iki.yak.ts.compression.gorilla.LongArrayOutput;
import rx.Observable;

/**
 * DataPointCompressor for the type 02 (Gorilla V2)
 *
 * @author Michael Burman
 */
public class TempTableCompressTransformer implements Observable.Transformer<Row, CompressedPointContainer> {

    private long timeslice;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private GorillaCompressor compressor;

    public TempTableCompressTransformer(long timeslice) {
        this.timeslice = timeslice;
    }

    @Override
    public Observable<CompressedPointContainer> call(Observable<Row> dataRow) {
        ByteBufLongOutput out = new ByteBufLongOutput();

        TagsSerializer tagsSerializer = new TagsSerializer(timeslice);
        return dataRow
                .collect(CompressedPointContainer::new,
                        (container, r) -> {
                            MetricType<?> metricType = MetricType.fromCode(r.getByte(1));

                            if(initialized.compareAndSet(false, true)) {
                                byte gorillaHeader = CompressorHeader.getHeader(CompressorHeader.Compressor.GORILLA_V2, EnumSet.noneOf
                                        (CompressorHeader.GorillaSettings.class));

                                if(metricType == MetricType.AVAILABILITY || metricType == MetricType.COUNTER) {
                                    // COUNTER and AVAILABILITY use Long values
                                    gorillaHeader = CompressorHeader.getHeader(CompressorHeader.Compressor.GORILLA_V2,
                                            EnumSet.of(CompressorHeader.GorillaSettings.LONG_VALUES));
                                }
                                out.writeBits(Byte.valueOf(gorillaHeader).longValue(), 8);
                                compressor = new GorillaCompressor(timeslice, out);
                            }

                            // "SELECT tenant_id, type, metric, time, n_value, availability, l_value, tags FROM %s " +
                            long timestamp = r.getTimestamp(3).getTime(); // Check validity
                            switch(r.getByte(1)) {
                                case 0: // GAUGE
                                    compressor.addValue(timestamp, r.getDouble(4));
                                    break;
                                case 1: // AVAILABILITY
                                    compressor.addValue(timestamp, AvailabilityType.fromBytes(r.getBytes(5)).getCode());
                                    break;
                                case 2: // COUNTER
                                    compressor.addValue(timestamp, r.getLong(6));
                                    break;
                                default:
                                    // Not supported yet
                                    throw new RuntimeException("Metric of type " + r.getByte(1) + " is not supported" +
                                            " in compression");
                            }
                            Map<String, String> tags = r.getMap(7, String.class, String.class);
                            if(tags != null && !tags.isEmpty()) {
                                tagsSerializer.addDataPointTags(timestamp, tags);
                            }
                        })
                .doOnNext(cpc -> {
                    if(initialized.get()) {
                        compressor.close();

                        ByteBuffer valueBuffer = (ByteBuffer) out.getByteBuffer().flip();
                        ByteBuffer tagsBuffer = tagsSerializer.getByteBuffer();
                        tagsBuffer.flip();
                        cpc.setValueBuffer(valueBuffer);
                        if(tagsBuffer.limit() > 1) {
                            // Exclude header
                            cpc.setTagsBuffer(tagsBuffer);
                        }
                    }
                });
    }

    class ByteBufLongOutput extends LongArrayOutput {
        private ByteBuffer byteBuf;
        private LongBuffer lBuf;

        public ByteBufLongOutput() {
            byteBuf = ByteBuffer.allocateDirect(256 * Long.BYTES);
            lBuf = byteBuf.asLongBuffer();
        }

        @Override
        protected void expandAllocation() {
            ByteBuffer largerBB = ByteBuffer.allocateDirect(byteBuf.capacity()*2);
            byteBuf.position(lBuf.position() * Long.BYTES);
            byteBuf.flip();
            largerBB.put(byteBuf);
            largerBB.position(byteBuf.limit());
            byteBuf = largerBB;
            lBuf = largerBB.asLongBuffer();
        }

        @Override
        protected int capacityLeft() {
            return lBuf.capacity() - lBuf.position();
        }

        @Override
        protected void flipWordWithoutExpandCheck() {
            lBuf.put(lB);
            lB = 0;
            bitsLeft = Long.SIZE;
        }

        public ByteBuffer getByteBuffer() {
            byteBuf.limit(lBuf.position() * Long.BYTES);
            byteBuf.position(lBuf.position() * Long.BYTES);
            return this.byteBuf;
        }
    }
}
