package com.nishant.kafkabr.document;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Arrays;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;

@Document(collection = "ProducerTask")
public class ProducerTask {
    @Id
    private String id;
    private List<String> keys;
    private boolean assigned;
    private String bootstrapServers;
    private String topic;
    private String bucket;
    private String username;
    private String password;

    public ProducerTask(List<String> keys, String bootstrapServers, String topic, String bucket, String username, String password) {
        this.keys = keys;
        this.assigned = false;
        this.bootstrapServers = bootstrapServers;
        validate(Arrays.asList(bootstrapServers.split(",")));
        this.topic = topic;
        this.bucket = bucket;
        this.username = username;
        this.password = password;
    }

    private void validate(List<String> urls) {
        for (String url : urls) {
            String host = getHost(url);
            Integer port = getPort(url);
            if (host == null || port == null) {
                throw new ConfigException("Invalid url in " + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG + ": " + url);
            }
        }
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getId() {
        return id;
    }

    public boolean isAssigned() {
        return assigned;
    }

    public void setAssigned(boolean assigned) {
        this.assigned = assigned;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    @Override
    public String toString() {
        return "ProducerTask{" +
                "id='" + id + '\'' +
                ", keys=" + keys +
                ", assigned=" + assigned +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", bucket='" + bucket + '\'' +
                ", username='" + username + '\'' +
                '}';
    }

}
