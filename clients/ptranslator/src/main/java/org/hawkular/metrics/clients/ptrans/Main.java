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

import static org.hawkular.metrics.clients.ptrans.OptionsFactory.CONFIG_FILE_OPT;
import static org.hawkular.metrics.clients.ptrans.OptionsFactory.HELP_OPT;
import static org.hawkular.metrics.clients.ptrans.OptionsFactory.PID_FILE_OPT;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jnr.posix.POSIXFactory;

/**
 * Simple client (proxy) that receives messages from various protocols
 * and forwards the data to the rest server.
 * Multiple protocols are supported.
 *
 * @author Heiko W. Rupp
 * @author Thomas Segismont
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private final String[] args;
    private final OptionsFactory optionsFactory;
    private PTrans ptrans;
    private PidFile pidFile;

    private Main(String[] args) {
        this.args = args;
        optionsFactory = new OptionsFactory();
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void start() {
        Options options = optionsFactory.getCommandOptions(true);
        Exception parseException = null;
        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(options, args, true);
        } catch (Exception e) {
            parseException = e;
        }
        boolean hasHelpOption = hasHelpOption();
        if (parseException != null) {
            if (!hasHelpOption) {
                System.err.println(parseException.getMessage());
            }
            printHelp();
            System.exit(hasHelpOption ? 0 : 1);
        }
        if (hasHelpOption) {
            printHelp();
            System.exit(0);
        }
        File configFile = new File(cmd.getOptionValue(CONFIG_FILE_OPT));
        if (!configFile.isFile()) {
            System.err.printf("Configuration file %s does not exist or is not readable.%n",
                configFile.getAbsolutePath());
            System.exit(1);
        }
        if (cmd.hasOption(PID_FILE_OPT)) {
            File file = new File(cmd.getOptionValue(PID_FILE_OPT));
            pidFile = new PidFile(file);
            boolean locked = pidFile.tryLock(POSIXFactory.getPOSIX().getpid());
            if (!locked) {
                System.exit(1);
            }
        }
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            properties.load(inputStream);
        } catch (IOException e) {
            System.err.printf("Unexpected error while reading configuration file %s%n", configFile.getAbsolutePath());
            e.printStackTrace();
            System.exit(1);
        }
        Configuration configuration = Configuration.from(properties);
        if (!configuration.isValid()) {
            System.err.println("Invalid configuration:");
            configuration.getValidationMessages().forEach(System.err::println);
            System.exit(1);
        }
        ptrans = new PTrans(configuration);
        ptrans.start();
    }

    private boolean hasHelpOption() {
        Options commandOptions = optionsFactory.getCommandOptions(false);
        CommandLine cmd;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(commandOptions, args, true);
            return cmd.hasOption(HELP_OPT);
        } catch (Exception e) {
            return false;
        }
    }

    private void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(Integer.MAX_VALUE); // Do not wrap
        formatter.printHelp("ptrans", optionsFactory.getCommandOptions(true), true);
    }

    private void stop() {
        try {
            if (ptrans != null) {
                ptrans.stop();
            }
        } finally {
            if (pidFile != null) {
                pidFile.release();
            }
        }
    }

    public static void main(String[] args) {
        Main main = new Main(args);
        try {
            main.start();
        } catch (Exception e) {
            LOG.error("Exception on startup", e);
            System.exit(1);
        }
    }
}
