// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.audit;

import com.google.common.base.Strings;
import org.apache.doris.common.util.DigitalVersion;
import org.apache.doris.plugin.AuditPlugin;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.PluginInfo.PluginType;
import org.apache.doris.plugin.PluginMgr;

import com.google.common.collect.Queues;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * This plugin will load audit log to specified doris table at specified interval
 */
public class AuditKafkaPlugin extends Plugin implements AuditPlugin {
    private static final Logger LOG = LogManager.getLogger(AuditKafkaPlugin.class);

    private static final Gson GSON = new GsonBuilder().setExclusionStrategies(new ExclusionStrategy() {
        @Override
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(AuditEvent.AuditField.class)  == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }).create();

    // sometimes the audit log may fail to load to doris, count it to observe.
    private long discardLogNum = 0;
    private String brokersUrl;
    private String topicName;
    private String clusterName;

    private BlockingQueue<AuditEvent> auditEventQueue;
    private Producer<String, String> producer;
    private Thread loadThread;

    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    private final PluginInfo pluginInfo;

    public AuditKafkaPlugin() {
        pluginInfo = new PluginInfo(PluginMgr.BUILTIN_PLUGIN_PREFIX + "AuditKafka", PluginType.AUDIT,
                "add audit kafka, to send audit log to kafka top", DigitalVersion.fromString("1.0.0"),
                DigitalVersion.fromString("1.8.31"), AuditKafkaPlugin.class.getName(), null, null);
    }

    public PluginInfo getPluginInfo() {
        return pluginInfo;
    }

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            // make capacity large enough to avoid blocking.
            // and it will not be too large because the audit log will flush if num in queue is larger than
            // GlobalVariable.audit_plugin_max_batch_bytes.
            loadConfig(ctx, info.getProperties());
            this.auditEventQueue = Queues.newLinkedBlockingDeque(100000);
            this.producer = KafkaClient.getProducer(this.brokersUrl);
            this.loadThread = new Thread(new LoadWorker(this.producer), "audit kafka thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("encounter exception when closing the audit loader", e);
                }
            }
        }
    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath());
        if (!Files.exists(pluginPath)) {
            throw new PluginException("plugin path does not exist: " + pluginPath);
        }

        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile)) {
            throw new PluginException("plugin conf file does not exist: " + confFile);
        }

        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile)) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }

        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        this.clusterName = props.getProperty("cluster");
        this.brokersUrl = props.getProperty("kafka.brokers");
        this.topicName = props.getProperty("kafka.topic");
    }

    public boolean eventFilter(AuditEvent.EventType type) {
        return type == AuditEvent.EventType.AFTER_QUERY;
    }

    public void exec(AuditEvent event) {
        if (!event.isQuery || Strings.isNullOrEmpty(event.workloadGroup) || Strings.isNullOrEmpty(event.clientIp)) return;
        try {
            auditEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            ++discardLogNum;
            if (LOG.isDebugEnabled()) {
                LOG.debug("encounter exception when putting current audit batch, discard current audit event."
                        + " total discard num: {}", discardLogNum, e);
            }
        }
    }

    private String toJsonString(AuditEvent event) {
        JsonElement jsonElement = GSON.toJsonTree(event);
        jsonElement.getAsJsonObject().addProperty("Cluster", clusterName);
        return GSON.toJson(jsonElement);
    }

    private class LoadWorker implements Runnable {
        private Producer<String, String> producer;

        public LoadWorker(Producer<String, String> producer) {
            this.producer = producer;
        }

        public void run() {
            while (!isClosed) {
                try {
                    AuditEvent event = auditEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        String log = toJsonString(event);
                        producer.send(new ProducerRecord<>(topicName, log));
                        if (LOG.isDebugEnabled()){
                            LOG.debug("kafka sending {} to {}", log, topicName);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("run audit kafka error:", e);
                }
            }
        }
    }

    private static class KafkaClient {
        public static KafkaProducer<String, String> getProducer(String brokers)
        {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
            properties.put(ProducerConfig.ACKS_CONFIG, "1");
            properties.put(ProducerConfig.RETRIES_CONFIG, "3");
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "33554432");
            return new KafkaProducer<>(properties);
        }
    }
}
