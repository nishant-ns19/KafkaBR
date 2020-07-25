FROM openjdk:8-jdk-alpine
LABEL author="Nishant Singh"
ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar","--spring.data.mongodb.uri=${MONGODB_URI}"]