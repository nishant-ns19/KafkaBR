package com.nishant.kafkabr;

import com.mongodb.client.result.UpdateResult;
import com.nishant.kafkabr.document.AWSCredentials;
import com.nishant.kafkabr.document.ConsumerGroups;
import com.nishant.kafkabr.document.ProducerTask;
import com.nishant.kafkabr.utils.MyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Component
public class MongoHelper {
    private final MongoTemplate mongoTemplate;

    @Autowired
    public MongoHelper(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void initIndexes() {
        mongoTemplate.indexOps(MyUtils.PRODUCER_TASK_COLLECTION).ensureIndex(new Index().on(MyUtils.PRODUCER_TASK_ASSIGNED, Sort.Direction.ASC));
    }

    public boolean saveConsumer(ConsumerGroups consumerGroups) {
//        add this topic to mongo only if its not being backed up already
        Query query = new Query(Criteria.where(MyUtils.CONSUMER_GROUPS_ID).is(consumerGroups.getGroupId()));
        Update update = new Update();
        update.setOnInsert(MyUtils.CONSUMER_GROUPS_ID, consumerGroups.getGroupId());
        update.setOnInsert(MyUtils.CONSUMER_GROUPS_TOPIC, consumerGroups.getTopic());
        update.setOnInsert(MyUtils.CONSUMER_GROUPS_BOOTSTRAP_SERVERS, consumerGroups.getBootstrapServers());
        update.setOnInsert(MyUtils.CONSUMER_GROUPS_USERNAME, consumerGroups.getUsername());
        update.setOnInsert(MyUtils.CONSUMER_GROUPS_PASSWORD, consumerGroups.getPassword());
        UpdateResult result = mongoTemplate.upsert(query, update, ConsumerGroups.class, MyUtils.CONSUMER_GROUP_COLLECTION);
        return (result.getUpsertedId() != null);
    }

    public List<ConsumerGroups> findAllConsumers() {
        return mongoTemplate.findAll(ConsumerGroups.class, MyUtils.CONSUMER_GROUP_COLLECTION);
    }

    public void saveTask(ProducerTask producerTask) {
        mongoTemplate.save(producerTask, MyUtils.PRODUCER_TASK_COLLECTION);
    }

    public ProducerTask findNewTask() {
//        find a new task and return it and at the same time change its status
        Query query = new Query();
        query.addCriteria(Criteria.where(MyUtils.PRODUCER_TASK_ASSIGNED).is(false));
        Update update = new Update();
        update.set(MyUtils.PRODUCER_TASK_ASSIGNED, true);
        return mongoTemplate.findAndModify(query, update, ProducerTask.class, MyUtils.PRODUCER_TASK_COLLECTION);
    }

    public void renewAllTasks() {
//        change assigned status to false for all tasks on startup
        Query query = new Query();
        query.addCriteria(Criteria.where(MyUtils.PRODUCER_TASK_ASSIGNED).is(true));
        Update update = new Update();
        update.set(MyUtils.PRODUCER_TASK_ASSIGNED, false);
        mongoTemplate.updateMulti(query, update, ProducerTask.class, MyUtils.PRODUCER_TASK_COLLECTION);
    }

    public void deleteTask(ProducerTask producerTask) {
//        delete task after it is complete
        Query query = new Query(Criteria.where(MyUtils.PRODUCER_TASK_ID).is(producerTask.getId()));
        mongoTemplate.remove(query, ProducerTask.class);
    }

    public AWSCredentials getAWSCredentials() {
        return mongoTemplate.findOne(new Query(), AWSCredentials.class);
    }


}
