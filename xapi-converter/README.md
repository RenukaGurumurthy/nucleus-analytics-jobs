# Experience API Converter
This migration job will convert all the Gooru events into xAPI format.

## Prerequisites

- Talend Open Studio
- Java 8

## Running Build via Talend
1. Open Talend Open Studio.
2. Download and import xApiConverter.zip file as a exisiting project.
3. Click Run icon.
## Running Build as Bash
1. Download and unzip xApiConverterBash.zip.
2. Go to extracted folder. If you're using Linux maching exectue Experience_Api_Converter_run.sh Or Windows machine Experience_Api_Converter_run.bat.
3. Make sure the properties are correct inside the Experience_Api_Converter/experienceapiconverter/experience_api_converter_0_1/contexts.
4. You can choose properties while executing sh/bat script. Eg : sh Experience_Api_Converter_run.sh -context=Production (This command will use Production properties.)
## Export As Standalone Project (Recommended method)
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
