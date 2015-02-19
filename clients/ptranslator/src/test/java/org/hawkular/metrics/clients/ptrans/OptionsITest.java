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
package org.hawkular.metrics.clients.ptrans;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * @author Thomas Segismont
 */
public class OptionsITest extends ExecutableITestBase {

    @Test
    public void shouldExitWithErrorIfConfigPathIsMissing() throws Exception {
        ptransProcess = ptransProcessBuilder.start();
        int returnCode = ptransProcess.waitFor();
        assertThat(returnCode).isNotEqualTo(0);

        assertThat(ptransErr).isFile().canRead().hasContent("Missing required option: c");
    }

    @Test
    public void shouldExitWithHelpIfOptionIsPresent() throws Exception {
        ptransProcessBuilder.command().add("-h");

        ptransProcess = ptransProcessBuilder.start();
        int returnCode = ptransProcess.waitFor();
        assertThat(returnCode).isEqualTo(0);

        assertThat(ptransErr).isFile().canRead().hasContent("");
    }
}
