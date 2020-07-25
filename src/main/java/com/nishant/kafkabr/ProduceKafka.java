package com.nishant.kafkabr;

import com.nishant.kafkabr.document.ProducerTask;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProduceKafka extends Thread {

    private final AtomicBoolean isRunning;
    private final S3Controller s3Controller;
    private final int id;
    private KafkaProducer<String, String> producer;
    private MongoHelper mongoHelper;

    public ProduceKafka(int id) {
        this.id = id;
        this.producer = null;
        mongoHelper = KafkabrApplication.getAppContext().getBean(MongoHelper.class);
        s3Controller = new S3Controller();
        isRunning = new AtomicBoolean(true);
    }

    private void commit(ProducerTask producerTask) {
//        try deleting the task indefinitely as its complete now
        while (true) {
            try {
                System.out.println("Deleting Task: " + producerTask.toString());
                mongoHelper.deleteTask(producerTask);
                producer.close();
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                System.out.println("Retrying..");
                try {
                    sleep(2000);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private void produce(List<String> recordList, String topic) {
        for (String s : recordList) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, s);
            try {
                producer.send(record);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void configureProducer(ProducerTask task) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        String temp = "org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"" + task.getUsername() + "\"   password=\"" + task.getPassword() + "\";";
        properties.put("sasl.jaas.config", temp);
        properties.put("ssl.endpoint.identification.algorithm", "https");
        properties.put("sasl.mechanism", "PLAIN");
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            try {
                ProducerTask currentTask = mongoHelper.findNewTask();
                if (currentTask != null) {
                    System.out.println("Assigning task: " + currentTask.toString() + " to thread: " + id);
                    configureProducer(currentTask);
                    List<String> keyList = currentTask.getKeys();
                    keyList.parallelStream().forEach(key -> {
                        if (isRunning.get()) {
                            List<String> recordList = s3Controller.downloadObjects(currentTask.getBucket(), key);
                            produce(recordList, currentTask.getTopic());
                        }
                    });
                    if (isRunning.get()) {
                        commit(currentTask);
                    }
                } else {
                    System.out.println("Producer " + id + " Paused");
                    try {
                        sleep(10000);
                    } catch (InterruptedException ignored) {
                    }
                    System.out.println("Producer " + id + " Resumed");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void stopThread() {
        System.out.println("Producer Stopped: " + id);
        isRunning.set(false);
    }
}
