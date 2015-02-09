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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Creates the command line options configuration.
 *
 * @author Thomas Segismont
 */
class OptionsFactory {
    static final String HELP_OPT = "h";
    static final String HELP_LONGOPT = "help";
    static final String CONFIG_FILE_OPT = "c";
    static final String CONFIG_FILE_LONGOPT = "config-file";
    static final String PID_FILE_OPT = "p";
    static final String PID_FILE_LONGOPT = "pid-file";

    /**
     * @param withRequiredFlags true if required options should be marked as such
     * @return the command line options configuration
     */
    Options getCommandOptions(boolean withRequiredFlags) {
        Options options = new Options();
        options.addOption(HELP_OPT, HELP_LONGOPT, false, "Print usage and exit.");
        Option configFileOption = new Option(CONFIG_FILE_OPT, CONFIG_FILE_LONGOPT, true,
            "Set the path to the configuration file.");
        configFileOption.setRequired(withRequiredFlags);
        options.addOption(configFileOption);
        options.addOption(PID_FILE_OPT, PID_FILE_LONGOPT, true, "Set the path to the PID file.");
        return options;
    }
}
