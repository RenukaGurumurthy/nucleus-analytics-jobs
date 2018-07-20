This job is used to extract the login count data per tenant by different frequencies.

###Build Information
This repository uses Gradle to build the shadow jar as default target. The shadow jar is created in ${project_root}/build/libs directory.

The configuration is governed by a JSON file. A sample JSON file is bundled in ${project_root}/src/main/resources/config.json

###Run Information

> java -jar ${project_root}/build/libs/login-count-reports-0.1-snapshot-fat.jar ${project_root}/src/main/resources/config.json ${frequency} ${start_date} ${end_date}

Where,
> frequency:  Y (Yearly), Q(Quartely), M (Monthly), W (Weekly), D (Daily)

> start_date:  start date of the report. Format: YYYY-MM-DD

> end_date: end date of the report. Format: YYYY-MM-DD
