# README
### There are three document classes for mongoDB:
- AWSCredentials for structure of AWS Credentials
- ProducerTask for structure of Replay Tasks
- ConsumerGroups for storing meta data about consumer groups so that service can start backup for existing topics again on startup

### MyUtils Class 
This class has definitions for some utilities

### AppController Class
This class defines all the mappings used for interacting with service

### S3Controller Class
This class has definitions for some function which interact with S3 database

### MongoHelper Class
Functions defined in this class helps in interacting with MongoDB

### ProduceKafka Class
This class extends to thread and keep pinging mongoDB to check  whether there is any pending task or not

### ConsumeKafka Class
This class keeps polling Kafka Topic to check whether there are any records to be backed up

### KafkabrApplication Class
This class contains main functions and all necessary operations for startup

### Dockerfile
This file contains configurations for creating docker image

## CHANGES TO BE DONE FOR INTEGRATING (change as per your needs)
- ### Remove username and password field from ConsumerGroups class and ProducerTask class
- ### Remove key and pswd from parameters required by controller 
- ### Change groupd id from key#topic to bootstrapServers#topic in startListening mapping controller
- ### Change properties for consumers and producers in ConsumeKafka and ProduceKafka class respectively
- ### Change saveConsumer function in MongoHelper class by omitting username and password update
