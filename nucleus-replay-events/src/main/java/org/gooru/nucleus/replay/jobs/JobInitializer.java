package org.gooru.nucleus.replay.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Timer;

import org.gooru.nucleus.replay.jobs.constants.ConfigConstants;
import org.gooru.nucleus.replay.jobs.infra.CassandraClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInitializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobInitializer.class);
  private static CassandraClient cassandraClient;
  private static String API_END_POINT;
  private static Long cutOffTimeInMs;
  private static Long jobFrequencyInMs;

  public static void main(String args[]) throws Exception {
    Timer time = new Timer();
    if (args.length > 0) {
      JSONObject config = getConfig(args[0]);
      if (config != null && config.length() > 0) {
        if (config.isNull("cassandra")) {
          LOGGER.error("cassandra propertiesare mandatory..");
          System.exit(0);
        }
        cassandraClient = new CassandraClient(config);
        API_END_POINT = config.getString(ConfigConstants.API_END_POINT);
        cutOffTimeInMs = config.getLong(ConfigConstants.CUT_OFF_TIME);
        jobFrequencyInMs = config.getLong(ConfigConstants.JOB_FREQUENCY);
        // Default cut off time is 1 hour.
        cutOffTimeInMs = (cutOffTimeInMs == null ? 3600000 : cutOffTimeInMs);
        // Default frequency is 3 Minutes.
        jobFrequencyInMs = (jobFrequencyInMs == null ? 180000 : jobFrequencyInMs);

        ReplayEvents replayTask = new ReplayEvents(cassandraClient, API_END_POINT, cutOffTimeInMs);
        time.schedule(replayTask, 0, jobFrequencyInMs);
        System.out.println("Task submitted for every "+ jobFrequencyInMs + " ms");
      } else {
        LOGGER.error("Configs can not be empty");
        System.exit(0);
      }
    } else {
      LOGGER.error("Config file is required");
      System.exit(0);
    }
  }

  private static JSONObject getConfig(String configFileLocation) {
    JSONObject configJSON = null;
    String jsonData = "";
    BufferedReader br = null;
    try {
      String line;
      br = new BufferedReader(new FileReader(configFileLocation));
      while ((line = br.readLine()) != null) {
        jsonData += line + "\n";
      }
    } catch (IOException e) {
      LOGGER.error("Unable to process JSON from configuration file ", e);
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException ex) {
        LOGGER.error("Unable to get JSON from configuration file ", ex);
      }
    }
    if (!jsonData.isEmpty()) {
      configJSON = new JSONObject(jsonData);
    }
    return configJSON;
  }
}
