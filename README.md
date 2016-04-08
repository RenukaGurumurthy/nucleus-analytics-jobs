# gooru-migration-scripts
This project contains all type of migration scripts.

## Prerequisites

- Gradle 2.7
- Java 8

## Running Build

The default task is *shadowJar* which is provided by plugin. So running *gradle* from command line will run *shadowJar* and it will create a fat jar in build/libs folder. Note that there is artifact name specified in build file and hence it will take the name from directory housing the project.

Once the far Jar is created, it could be run as any other Java application.

## Running the Jar as an application

Following command could be used, from the base directory.

> java -classpath build/libs/migration-scripts-fat.jar: org.gooru.migration.jobs.StatDataMigration
