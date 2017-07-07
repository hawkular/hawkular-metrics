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
package org.hawkular.metrics.core.service.compress;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * CompressionHeader for compression details, stored as the first byte in every c_value ByteBuffer
 *
 * First 4 bits are reserved for compressor type
 * Next 4 bits are reserved for compressor's properties
 *
 * @author Michael Burman
 */
public class CompressorHeader {

    public interface CompressorSetting {
        byte getByteValue();
    }

    public enum Compressor {
        GORILLA((byte) 0x10, GorillaSettings.class),
        GORILLA_V2((byte) 0x20, GorillaSettings.class);

        private byte value;

        @SuppressWarnings("rawtypes")
        private Class enumClass;

        @SuppressWarnings("rawtypes")
        Compressor(byte value, Class enumClass) {
            this.value = value;
            this.enumClass = enumClass;
        }

        public byte getByteValue() {
            return this.value;
        }

        @SuppressWarnings("rawtypes")
        public <E extends Enum<E> & CompressorSetting> Class getSettingsClass() {
            return enumClass;
        }
    }

    public enum GorillaSettings implements CompressorSetting {
        SECOND_PRECISION((byte) 0x01), LONG_VALUES((byte) 0x02);

        private byte value;

        GorillaSettings(byte value) {
            this.value = value;
        }

        public byte getByteValue() {
            return this.value;
        }
    }

    public static byte getHeader(Compressor compressor, EnumSet<? extends CompressorSetting> settings) {
        byte b = (byte) (compressor.getByteValue() & 0xF0);
        for (CompressorSetting setting : settings) {
            b ^= setting.getByteValue();
        }

        return b;
    }

    public static Compressor getCompressor(byte b) {
        byte d = (byte) (b & 0xF0);

        for (Compressor compressor : Compressor.values()) {
            if(compressor.value == d) {
                return compressor;
            }
        }

        throw new RuntimeException("Invalid compression method " + Integer.toBinaryString((d & 0xFF) + 0x100)
                .substring(1));

    }

    public static <E extends Enum<E> & CompressorSetting> EnumSet getSettings(Class<E> e, byte b) {
        List<E> enumList = new ArrayList<>(4); // There's only 4 bits reserved
        EnumSet<E> enumSet = EnumSet.allOf(e);
        for (E setting : enumSet) {
            if((b & setting.getByteValue()) == setting.getByteValue()) {
                enumList.add(setting);
            }
        }

        return EnumSet.copyOf(enumList);
    }
}
