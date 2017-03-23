# nucleus-analytics-jobs
This project contains all type of migration scripts.

## Prerequisites

- Gradle 2.7
- Java 8

## Running Build

The default task is *shadowJar* which is provided by plugin. So running *gradle build shadow* from command line will run *shadowJar* and it will create a fat jar in build/libs inside respective jobs folder. Note that there is artifact name specified in build file and hence it will take the name from directory housing the project.

Once the far Jar is created, it could be run as any other Java application.

## Running the Jar as an application

Following command could be used, from the base directory.

> java -cp nucleus-consumer-sync-jobs/build/libs/nucleus-consumer-sync-jobs-0.1-snapshot-fat.jar: org.gooru.nucleus.consumer.sync.jobs.JobInitializer nucleus-consumer-sync-jobs/src/main/resources/nucleus-consumer-sync-jobs-config.json

### Comments

Since we implemented Kafka consumer to consume messages, Need not to form cluster. Please make sure correct Kafka topic and group ID. If you want to deploy same handlers multiple time to handle the request traffic, group ID should be same. So that it will be act as loadbalancer.

> java -cp /tmp/nucleus-replay-events-0.1-snapshot-fat.jar: org.gooru.nucleus.replay.jobs.JobInitializer ~/nucleus-replay-config.json 201505060835 201703231600

### Comments
In this job arguments order should not change. First arguments would be config file location and then start & end time in yyyymmddhhmm format.
