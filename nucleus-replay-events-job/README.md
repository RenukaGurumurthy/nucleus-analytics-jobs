# nucleus-replay-events-job
This job is to process older events
## Prerequisites

- Java 8
- Copy the log files in preffered location. 
- Log files should be in .csv format. Refer the sample file (sample_logapi_log.csv).
- Property files will be available in this location (PostEvents/learning/postevents_0_1/contexts/). You can modify the configuration here.
- Sample files can be generated following two commands in old logapi server.
- Go to tomcat's event logs location (/opt/tomcat/event_api_logs).

> zgrep "Field :" *.gz | sed 's/.*Field\ \:\ //' | grep "collection.play\|collection.resource.play" 

> grep "Field :" *.log | sed 's/.*Field\ \:\ //' | grep "collection.play\|collection.resource.play" 

## Job Execution

- Download PostEvents_0.1.zip file
- Execute unzip command to explore the zipped file (unzip PostEvents_0.1.zip)
- Once unzip completed, go to PostEvents folder and find PostEvents_run.sh file.
- Execute the job using shell command. sh PostEvents_run.sh -context=<environment>.
- Example : sh PostEvents_run.sh -context=Dev .
