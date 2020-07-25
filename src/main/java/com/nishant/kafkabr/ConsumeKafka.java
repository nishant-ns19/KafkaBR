package com.nishant.kafkabr;

import com.nishant.kafkabr.document.ConsumerGroups;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumeKafka extends Thread {
    private final int MIN_BATCH_SIZE = 1000;
    private final String MAX_POLL_SIZE = "500";
    private final String groupID;
    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private final S3Controller s3Controller;
    private final AtomicBoolean isRunning;
    private final String username;

    public ConsumeKafka(ConsumerGroups consumerGroup) {
//        configure consumer
        this.groupID = consumerGroup.getGroupId();
        this.topic = consumerGroup.getTopic();
        this.username = consumerGroup.getUsername();
        Properties properties;
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerGroup.getBootstrapServers());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_SIZE);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        String temp = "org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"" + consumerGroup.getUsername() + "\"   password=\"" + consumerGroup.getPassword() + "\";";
        properties.put("sasl.jaas.config", temp);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("sasl.mechanism", "PLAIN");
        System.out.println("Initialising Consumer: " + this.groupID);
        consumer = new KafkaConsumer<>(properties);
//        calling list topics to check is connection has established or not
        consumer.listTopics();
        consumer.subscribe(Collections.singletonList(this.topic));
        s3Controller = new S3Controller();
        isRunning = new AtomicBoolean(true);
        System.out.println("Initialization Successful for Consumer: " + this.groupID);
    }

    @Override
    public void run() {
        System.out.println("Starting Consumer Group: " + groupID);
        ArrayList<String> buffer = new ArrayList<>();
        while (isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> buffer.add(record.value()));
//                upload file to S3
                if (buffer.size() >= MIN_BATCH_SIZE) {
                    System.out.println("Uploading Buffer to S3 for (" + groupID + "): " + buffer.size());
                    s3Controller.uploadObject(topic, username, buffer);
                    System.out.println("Committing offset for consumer: " + groupID);
                    consumer.commitSync();
                    System.out.println("Clearing buffer: " + groupID);
                    buffer.clear();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        closeConsumer();
    }

    public void stopThread() {
        isRunning.set(false);
        System.out.println("Consumer Stopped: " + groupID);
    }

    public void closeConsumer() {
        consumer.close();
    }

}
