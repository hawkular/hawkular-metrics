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
package org.hawkular.metrics.embedded;

import static java.util.Arrays.asList;
import static org.hawkular.metrics.embedded.EmbeddedConstants.CASSANDRA_CONFIG;
import static org.hawkular.metrics.embedded.EmbeddedConstants.CASSANDRA_LISTEN_ADDRESS_DEFAULT;
import static org.hawkular.metrics.embedded.EmbeddedConstants.CASSANDRA_NATIVE_PORT_DEFAULT;
import static org.hawkular.metrics.embedded.EmbeddedConstants.CASSANDRA_YAML;
import static org.hawkular.metrics.embedded.EmbeddedConstants.JBOSS_DATA_DIR;
import static org.hawkular.metrics.embedded.EmbeddedConstants.HAWKULAR_METRICS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * @author Stefan Negrea
 * @author John Sanda
 */
public class ConfigEditor {

    private File configFile;
    private Yaml yaml;
    private Map<String, Object> config;

    public ConfigEditor() {
    }

    @SuppressWarnings({ "unchecked" })
    public ConfigEditor(File cassandraYamlFile) throws IOException {
        configFile = cassandraYamlFile;
        try (FileInputStream inputStream = new FileInputStream(configFile)) {
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            yaml = new Yaml(options);
            config = (Map<String, Object>) yaml.load(inputStream);
        }
    }

    @SuppressWarnings({ "unchecked" })
    public ConfigEditor(InputStream inputStream, File destFile) throws IOException {
        try (InputStream yamlInputStream = inputStream) {
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            yaml = new Yaml(options);
            config = (Map<String, Object>) yaml.load(yamlInputStream);
            configFile = destFile;
        }
    }

    @SuppressWarnings("unchecked")
    public void initEmbeddedConfiguration() throws Exception {

        File basedir = new File(System.getProperty(JBOSS_DATA_DIR, "./"), HAWKULAR_METRICS);
        File confDir = new File(basedir, "conf");
        File yamlFile = new File(confDir, CASSANDRA_YAML);

        if (!yamlFile.exists()) {
            // load the default configuration file
            try (InputStream yamlInputStream =  getClass().getResourceAsStream("/" + CASSANDRA_YAML)) {
                DumperOptions options = new DumperOptions();
                options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
                yaml = new Yaml(options);
                config = (Map<String, Object>) yaml.load(yamlInputStream);
                configFile = yamlFile;
            }

            // set embedded configuration
            this.setDataFileDirectories(asList(new File(basedir, "data").getAbsolutePath()));
            this.setCommitLogDirectory(new File(basedir, "commitlog").getAbsolutePath());
            this.setSavedCachesDirectory(new File(basedir, "saved_caches").getAbsolutePath());

            this.setClusterName(HAWKULAR_METRICS);
            this.setSeeds(CASSANDRA_LISTEN_ADDRESS_DEFAULT);
            this.setListenAddress(CASSANDRA_LISTEN_ADDRESS_DEFAULT);
            this.setRpcAddress(CASSANDRA_LISTEN_ADDRESS_DEFAULT);
            this.setNativeTransportPort(CASSANDRA_NATIVE_PORT_DEFAULT);

            this.setKeyCacheSizeMb(getDefaultKeyCacheSize());
            this.setNativeTransportMaxThreads(4);
            this.setCompactionThroughputMbPerSec(0);
            this.setStartRpc(false);
            this.setHintedHandoffEnabled(false);
            this.setNumTokens(1);
            this.setRpcServerType("hsha");

            // create folders
            if (!basedir.exists()) {
                basedir.mkdir();
            }

            if (!confDir.exists()) {
                confDir.mkdir();
            }

            File tempFile = new File(this.getDataFileDirectories().get(0));
            if (!tempFile.exists()) {
                tempFile.mkdirs();
            }

            tempFile = new File(this.getCommitLogDirectory());
            if (!tempFile.exists()) {
                tempFile.mkdirs();
            }

            tempFile = new File(this.getSavedCachesDirectory());
            if (!tempFile.exists()) {
                tempFile.mkdirs();
            }

            this.save();
        }

        // set system properties
        System.setProperty(CASSANDRA_CONFIG, yamlFile.toURI().toURL().toString());
        System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0");
        System.setProperty("cassandra.start_rpc", "false");
    }

    public void save() throws IOException {
        if (configFile != null && yaml != null) {
            yaml.dump(config, new FileWriter(configFile));
        }

        yaml = null;
        config = null;
    }

    public String getClusterName() {
        return (String) config.get("cluster_name");
    }

    public void setClusterName(String clusterName) {
        config.put("cluster_name", clusterName);
    }

    public String getListenAddress() {
        return (String) config.get("listen_address");
    }

    public void setListenAddress(String address) {
        config.put("listen_address", address);
    }

    public String getRpcAddress() {
        return (String) config.get("rpc_address");
    }

    public void setRpcAddress(String address) {
        config.put("rpc_address", address);
    }

    public void setRpcServerType(String rpcServerType) {
        config.put("rpc_server_type", rpcServerType);
    }

    public String getRpcServerType() {
        return (String) config.get("rpc_server_type");
    }

    public String getAuthenticator() {
        return (String) config.get("authenticator");
    }

    public String getCommitLogDirectory() {
        return (String) config.get("commitlog_directory");
    }

    public void setCommitLogDirectory(String dir) {
        config.put("commitlog_directory", dir);
    }

    @SuppressWarnings("unchecked")
    public List<String> getDataFileDirectories() {
        return (List<String>) config.get("data_file_directories");
    }

    public void setDataFileDirectories(List<String> dirs) {
        config.put("data_file_directories", dirs);
    }

    public String getSavedCachesDirectory() {
        return (String) config.get("saved_caches_directory");
    }

    public void setSavedCachesDirectory(String dir) {
        config.put("saved_caches_directory", dir);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void setSeeds(String... seeds) {
        List seedProviderList = (List) config.get("seed_provider");
        Map seedProvider = (Map) seedProviderList.get(0);
        List paramsList = (List) seedProvider.get("parameters");
        Map params = (Map) paramsList.get(0);

        StringBuilder seedsString = new StringBuilder();
        for (int i = 0; i < seeds.length; i++) {
            if (i > 0) {
                seedsString.append(",");
            }

            seedsString.append(seeds[i]);
        }
        params.put("seeds", seedsString.toString());
    }

    public Integer getNativeTransportPort() {
        return (Integer) config.get("native_transport_port");
    }

    public void setNativeTransportPort(Integer port) {
        config.put("native_transport_port", port);
    }

    public void setCompactionThroughputMbPerSec(Integer compactionThroughput) {
        config.put("compaction_throughput_mb_per_sec", compactionThroughput);
    }

    public Integer getCompactionThroughputMbPerSec() {
        return (Integer) config.get("compaction_throughput_mb_per_sec");
    }

    public void setKeyCacheSizeMb(Integer keyCacheSizeMb) {
        config.put("key_cache_size_in_mb", keyCacheSizeMb);
    }

    public Integer getKeyCacheSizeMb() {
        return (Integer) config.get("key_cache_size_in_mb");
    }

    public void setHintedHandoffEnabled(Boolean hintedHandoffEnabled) {
        config.put("hinted_handoff_enabled", hintedHandoffEnabled);
    }

    public Boolean isHintedHandoffEnabled() {
        return (Boolean) config.get("hinted_handoff_enabled");
    }

    public void setNativeTransportMaxThreads(Integer nativeTransportMaxThreads) {
        config.put("native_transport_max_threads", nativeTransportMaxThreads);
    }

    public Integer getNativeTransportMaxThreads() {
        return (Integer) config.get("native_transport_max_threads");
    }

    public void setStartRpc(Boolean startRpc) {
        config.put("start_rpc", startRpc);
    }

    public boolean getStartRpc() {
        return Boolean.parseBoolean(config.get("start_rpc").toString());
    }

    public void setNumTokens(Integer numTokens) {
        config.put("num_tokens", numTokens);
    }

    public Integer getNumTokens() {
        return Integer.parseInt(config.get("num_tokens").toString());
    }

    private int getDefaultKeyCacheSize() {
        // cache size defaults to min(1% of Heap (in MB), 10 MB)
        return Math.min(Math.max(1, (int) (Runtime.getRuntime().totalMemory() * 0.01 / 1024 / 1024)), 10);
    }
}