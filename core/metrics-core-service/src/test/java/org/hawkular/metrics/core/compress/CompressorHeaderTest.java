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
package org.hawkular.metrics.core.compress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;

import org.hawkular.metrics.core.service.compress.CompressorHeader;
import org.junit.Test;

/**
 * @author Michael Burman
 */
public class CompressorHeaderTest {

    @Test
    public void testHeader() {
        EnumSet<CompressorHeader.GorillaSettings> settings =
                EnumSet.of(CompressorHeader.GorillaSettings.SECOND_PRECISION);

        byte gorillaHeader = CompressorHeader.getHeader(CompressorHeader.Compressor.GORILLA, settings);

        assertEquals(gorillaHeader, (byte) 0x11);

        CompressorHeader.Compressor compressor = CompressorHeader.getCompressor(gorillaHeader);
        assertEquals(CompressorHeader.Compressor.GORILLA, compressor);

        EnumSet<CompressorHeader.GorillaSettings> compressorSettings
                = CompressorHeader.getSettings(compressor.getSettingsClass(), gorillaHeader);

        assertEquals(1, compressorSettings.size());
        assertTrue(compressorSettings.contains(CompressorHeader.GorillaSettings.SECOND_PRECISION));
    }
}
