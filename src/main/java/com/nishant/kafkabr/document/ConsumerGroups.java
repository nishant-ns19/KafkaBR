package com.nishant.kafkabr.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "ConsumerGroups")
public class ConsumerGroups {

    @Id
    private String groupId;
    private String topic;
    private String bootstrapServers;
    private String username;
    private String password;

    public ConsumerGroups(String groupId, String topic, String bootstrapServers, String username, String password) {
        this.groupId = groupId;
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.username = username;
        this.password = password;
    }

    @Override
    public String toString() {
        return "ConsumerGroups{" +
                "groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", username='" + username + '\'' +
                '}';
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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
}
