package org.gooru.nucleus.consumer.sync.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.ConfigConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DataSourceRegistry;
import org.gooru.nucleus.consumer.sync.jobs.processors.MessageConsumer;
import org.gooru.nucleus.consumer.sync.jobs.processors.SyncCourseCompetencyTotalCount;
import org.gooru.nucleus.consumer.sync.jobs.processors.SyncStaticCourseCompetency;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobInitializer {
  private static KafkaConsumer<String, String> consumer = null;
  private static ExecutorService service = null;
  private static final Logger LOGGER = LoggerFactory.getLogger(JobInitializer.class);
  private static final Timer courseCompetencyTotalCountTimer = new Timer();
  private static final Timer staticCourseCompetencyTimer = new Timer();

  public static void main(String args[]) {
    if (args.length > 0) {
      JSONObject config = getConfig(args[0]);
      if (config != null && config.length() > 0) {
        service = Executors.newFixedThreadPool(10);
        DataSourceRegistry.getInstance().initializeComponent(config);
        // createConsumer(config);

        TimerTask courseCompetencyTotalCount = new SyncCourseCompetencyTotalCount(config);
        TimerTask staticCompetencyCount = new SyncStaticCourseCompetency(config);

        if (!config.isNull(AttributeConstants.SYNC_COURSE_COMPETENCY_TOTAL_COUNT)) {
          JSONObject courseCompetencyCountConfig = config.getJSONObject(AttributeConstants.SYNC_COURSE_COMPETENCY_TOTAL_COUNT);
          LOGGER.debug("SyncCourseCompetencyTotalCount : {} ", courseCompetencyCountConfig);
          courseCompetencyTotalCountTimer.scheduleAtFixedRate(courseCompetencyTotalCount, 500,
                  courseCompetencyCountConfig.isNull(AttributeConstants.DELAY) ? 100000
                          : courseCompetencyCountConfig.getLong(AttributeConstants.DELAY));
        }
        if (!config.isNull(AttributeConstants.SYNC_STATIC_COURSE_COMPETENCY)) {
          JSONObject staticCourseCompetencyConfig = config.getJSONObject(AttributeConstants.SYNC_STATIC_COURSE_COMPETENCY);
          LOGGER.debug("SyncStaticCourseCompetency : {} ", staticCourseCompetencyConfig);
          staticCourseCompetencyTimer.scheduleAtFixedRate(staticCompetencyCount, 500,
                  staticCourseCompetencyConfig.isNull(AttributeConstants.DELAY) ? 100000
                          : staticCourseCompetencyConfig.getLong(AttributeConstants.DELAY));
        }

      } else {
        LOGGER.error("Configs can not be empty");
        System.exit(0);
      }
    } else {
      LOGGER.error("Config file location is required");
      System.exit(0);
    }
  }

  private static void createConsumer(JSONObject config) {
    LOGGER.debug("Configuring KAFKA consumer.....");
    Properties props = new Properties();
    JSONObject kafkaConfig = config.getJSONObject(ConfigConstants.CONFIG_KAFKA);
    props.put(ConfigConstants.CONFIG_KAFKA_SERVERS, kafkaConfig.getString(ConfigConstants.CONFIG_KAFKA_SERVERS));
    props.put(ConfigConstants.CONFIG_KAFKA_TIME_OUT_IN_MS, kafkaConfig.getString(ConfigConstants.CONFIG_KAFKA_TIME_OUT_IN_MS));
    props.put(ConfigConstants.CONFIG_KAFKA_GROUP, kafkaConfig.getString(ConfigConstants.CONFIG_KAFKA_GROUP));
    props.put(ConfigConstants.CONFIG_KAFKA_KEY_DESERIALIZER, StringDeserializer.class.getName());
    props.put(ConfigConstants.CONFIG_KAFKA_VALUE_DESERIALIZER, StringDeserializer.class.getName());
    consumer = new KafkaConsumer<>(props);
    String[] topics = kafkaConfig.getString(ConfigConstants.CONFIG_KAFKA_TOPICS).split(",");
    consumer.subscribe(Arrays.asList(topics));
    service.submit(new MessageConsumer(consumer, config, service));
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
