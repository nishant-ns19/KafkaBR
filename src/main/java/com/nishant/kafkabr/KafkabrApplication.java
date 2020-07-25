package com.nishant.kafkabr;

import com.nishant.kafkabr.document.AWSCredentials;
import com.nishant.kafkabr.document.ConsumerGroups;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class KafkabrApplication {

    private static final int MAX_PRODUCER = 50;
    public static ArrayList<ConsumeKafka> activeConsumers;
    private static ArrayList<ProduceKafka> activeProducers;
    private static ConfigurableApplicationContext ctx;
    private static AWSCredentials AWS_CREDS;

    public static void main(String[] args) {
        ctx = SpringApplication.run(KafkabrApplication.class, args);

        MongoHelper mongoHelper = ctx.getBean(MongoHelper.class);

//      get credentials for S3 from mongo
        AWS_CREDS = mongoHelper.getAWSCredentials();

//      on startup change state on of all tasks to unassigned
        mongoHelper.renewAllTasks();

//      find if there are any existing consumer groups and start them
        List<ConsumerGroups> existingConsumers = mongoHelper.findAllConsumers();
        activeConsumers = new ArrayList<>();
        existingConsumers.forEach(consumer -> {
            ConsumeKafka consumeKafka = new ConsumeKafka(consumer);
            addConsumerThread(consumeKafka);
        });

//      start some producer threads for auto assignment of tasks
        activeProducers = new ArrayList<>();
        for (int i = 0; i < MAX_PRODUCER; i++) {
            ProduceKafka produceKafka = new ProduceKafka(i);
            addProducerThread(produceKafka);
        }
    }

//    this function provide context to non spring classes so that they can also get beans
    public static ConfigurableApplicationContext getAppContext() {
        return ctx;
    }

    public static void addConsumerThread(ConsumeKafka consumeKafka) {
        consumeKafka.start();
        activeConsumers.add(consumeKafka);
    }

    private static void addProducerThread(ProduceKafka produceKafka) {
        produceKafka.start();
        activeProducers.add(produceKafka);
    }

    public static AWSCredentials getCredentials() {
        return AWS_CREDS;
    }

    @PreDestroy
    public void destroy() {
        activeProducers.parallelStream().forEach(ProduceKafka::stopThread);
        activeConsumers.parallelStream().forEach(ConsumeKafka::stopThread);
        try {
            Thread.sleep(20000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Shutting down");
    }

}
