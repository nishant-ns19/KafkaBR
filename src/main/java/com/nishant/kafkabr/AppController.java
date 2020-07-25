package com.nishant.kafkabr;

import com.nishant.kafkabr.document.ConsumerGroups;
import com.nishant.kafkabr.document.ProducerTask;
import com.nishant.kafkabr.utils.MyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Controller
public class AppController {

    @Autowired
    private MongoHelper mongoHelper;
    private final int objectClubSize = 100;

    @RequestMapping("/backup")
    @ResponseBody
    public void startListening(@RequestParam("bootstrapServers") String bootstrapServers, @RequestParam("topic") String topic, @RequestParam("key") String key, @RequestParam("pswd") String pswd, HttpServletResponse response) throws IOException {
        PrintWriter writer = response.getWriter();
        try {
            String groupID = key + "#" + topic;
            ConsumerGroups consumerGroups = new ConsumerGroups(groupID, topic, bootstrapServers, key, pswd);
            ConsumeKafka current = new ConsumeKafka(consumerGroups);
//            result will tell if this topic is being backed up already or not
            boolean result = mongoHelper.saveConsumer(consumerGroups);
            if (result) {
                KafkabrApplication.addConsumerThread(current);
                writer.println("Started consumer for:");
                writer.println("Bootstrap Servers: " + bootstrapServers);
                writer.println("Topic: " + topic);
                writer.println("with consumer group: " + groupID);
            } else {
                current.closeConsumer();
                writer.println("Consumer Group for the given topic already exists");
            }
        } catch (Exception e) {
            e.printStackTrace();
            writer.println(e.getMessage());
        }
    }

    @RequestMapping("/replay")
    @ResponseBody
    public void generate(@RequestParam("bucket") String bucket, @RequestParam("dir") String dir, @RequestParam("bootstrapServers") String bootstrapServers, @RequestParam("topic") String topic, @RequestParam("from") String from, @RequestParam("to") String to, @RequestParam("key") String key, @RequestParam("pswd") String pswd, HttpServletResponse response) throws IOException {
        PrintWriter writer = response.getWriter();
        try {
            Date start = new SimpleDateFormat("dd/MM/yyyy").parse(from);
            Date end = new SimpleDateFormat("dd/MM/yyyy").parse(to);
            if (end.before(start)) {
                throw new Exception("Date interval not valid");
            }
            S3Controller s3Controller = new S3Controller();
            List<String> listKeys = s3Controller.listKeys(bucket, dir, start, end);
            List<List<String>> partitions = MyUtils.partition(listKeys, objectClubSize);

//            upload tasks in parallel
            partitions.parallelStream().forEach(partition -> {
                System.out.println("Adding new task to mongo");
                mongoHelper.saveTask(new ProducerTask(partition, bootstrapServers, topic, bucket, key, pswd));
            });
            writer.println("New task uploaded successfully");
        } catch (Exception e) {
            e.printStackTrace();
            writer.println(e.getMessage());
        }
    }

    @RequestMapping("/list-consumers")
    @ResponseBody
    public void listConsumers(HttpServletResponse response) throws IOException {
        PrintWriter writer = response.getWriter();
        List<ConsumerGroups> list = mongoHelper.findAllConsumers();
        list.forEach(consumer -> writer.println(consumer.toString()));
    }
}
