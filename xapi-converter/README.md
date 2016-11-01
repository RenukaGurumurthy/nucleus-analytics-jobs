# Experience API Converter
This migration job will convert all the Gooru events into xAPI format.

## Prerequisites

- Apache Talend Open Studio
- Java 8

## Running Build via Talend
1. Open Talend Open Studion
2. Import this zip file as a exisiting project.
3. Click Run icon.

## Export As Standalone Project
1. Right click on project and click Build option. New window will open.
2. Choose Standalone project and export.
3. It will give shell script.
4. Execute this script using sh command. Eg: sh filename.sh

## Tables

### Gooru Event Cassandra

CREATE TABLE event_logger_insights.job_config (
    job_key text PRIMARY KEY,
    job_value text
);

CREATE TABLE event_logger_insights.events_v2 (
    event_auto_id text PRIMARY KEY,
    event_id text,
    fields text
);

### xAPI Event Casssandra

CREATE TABLE event_logger_insights.xapi_events (
    verb text,
    event_id text,
    event_text text,
    PRIMARY KEY (verb, event_id)
);
