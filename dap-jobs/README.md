    
## Build Information

This repo uses Gradle to build the shadow jar as default target. The shadow jar is created in ${project_root}/build/libs directory.

The configuration is governed by a JSON file. A sample JSON file is bundled in ${project_root}/src/main/resources/dap-jobs.json

To run the shadow jar

Learner profile initial setups:
==============================

### Nucleus 

> java -Djob.name=nucleus.user.competency.status -Ddata.ingestion.filepath=/tmp/lusd-profile-folders -Dconfig.file=./src/main/resources/dap-jobs.json -jar  ./build/libs/dap-jobs-fat.jar

### DAP

> java -Djob.name=dap.learner.profile.competency.status -Ddata.ingestion.filepath=/tmp/lusd-profile-folders -Dconfig.file=./src/main/resources/dap-jobs.json -jar  ./build/libs/dap-jobs-fat.jar

### RGO 

> java -Djob.name=user.competency.matrix -Ddata.ingestion.filepath=/tmp/lusd-profile-folders -Dconfig.file=./src/main/resources/dap-jobs.json -jar  ./build/libs/dap-jobs-fat.jar
