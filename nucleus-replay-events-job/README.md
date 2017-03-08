# nucleus-replay-events-job
This job is to process older events
## Prerequisites

- Java 8
- Copy the log files as in preffered location. 
- Log files should be .csv format.
- Property files will be available in this location (PostEvents/learning/postevents_0_1/contexts/). You can modify the configuration here.

## Running Build

- Download PostEvents_0.1.zip file
- Execute unzip command to explore the zipped file (unzip PostEvents_0.1.zip)
- Once unzip completed, go to PostEvents folder and find PostEvents_run.sh file.
- Execute the job using shell command. sh PostEvents_run.sh -context=<environment>.
- Example : sh PostEvents_run.sh -context=Dev .
