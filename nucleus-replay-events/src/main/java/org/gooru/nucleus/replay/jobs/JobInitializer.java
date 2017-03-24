package org.gooru.nucleus.replay.jobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.gooru.nucleus.replay.jobs.constants.AttributeConstants;
import org.gooru.nucleus.replay.jobs.constants.ConfigConstants;
import org.gooru.nucleus.replay.jobs.infra.CassandraClient;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class JobInitializer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobInitializer.class);
  private static CassandraClient cassandraClient;
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
  private static String API_END_POINT;

  public static void main(String args[]) throws Exception {
    // args = new String[] {
    // "/home/100041/work/nucleus-analytics-jobs/nucleus-replay-events/src/main/resources/nucleus-replay-events-config.json"
    // };

    if (args.length > 0) {
      JSONObject config = getConfig(args[0]);
      String start = null;
      String end = null;
      try {
        start = args[1];
        end = args[2];
      } catch (Exception e) {
        LOGGER.error("Start and End Time is Mandatory...");
        System.exit(0);
      }

      if (config != null && config.length() > 0) {
        cassandraClient = new CassandraClient(config);
        API_END_POINT = config.getString(ConfigConstants.API_END_POINT);
        // String start = "201609241600";
        // String end = "201609251600";

        Long startTime = minuteDateFormatter.parse(start).getTime();
        LOGGER.info("startTime : " + start);
        Long endTime = minuteDateFormatter.parse(end).getTime();
        LOGGER.info("endTime : " + end);

        for (Long startDate = startTime; startDate < endTime;) {
          String currentDate = minuteDateFormatter.format(new Date(startDate));
          LOGGER.info("Running for :" + currentDate);
          ResultSet eventIds = null;
          try {
            eventIds = findEventIdsByTime(currentDate);
          } catch (Exception e) {
            // FIXME:Today we can skip this exception. We need to look at it in
            // future.
            LOGGER.error("Unexpected Exception while getting eventIDs. Can't do anything here.." + e);
          }
          if (eventIds != null) {
            for (Row eventId : eventIds) {
              ResultSet events = null;
              try {
                events = findEventById(eventId.getString(AttributeConstants.EVENT_ID));
              } catch (Exception e) {
                // FIXME:Today we can skip this exception. We need to look at
                // it in future.
                LOGGER.error("Unexpected exception while getting event. Can't do anything here.." + e);
              }
              if (events != null) {
                for (Row event : events) {
                  String eventData = event.getString(AttributeConstants.FIELDS);
                  // LOGGER.info("Event : " + "[" + eventData + "]");
                  int status = postRequest("[" + eventData + "]");
                  LOGGER.info("Status:" + status);
                  if (status != 200) {
                    LOGGER.warn("Retrying request.....");
                    Thread.sleep(1000);
                    int retryStatus = postRequest("[" + eventData + "]");
                    LOGGER.info("retryStatus:" + retryStatus);

                  }
                }
              }
            }
          }

          // Incrementing time - one minute
          startDate = new Date(startDate).getTime() + 60000;
          Thread.sleep(200);
        }
        LOGGER.info("Process DONE. Closing job...");
        System.exit(0);
      } else {
        LOGGER.error("Configs can not be empty");
        System.exit(0);
      }
    } else

    {
      LOGGER.error("Config file location is required");
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

  private static ResultSet findEventIdsByTime(String eventTime) {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.EVENTS_TIMELINE)
            .where(QueryBuilder.eq(AttributeConstants.EVENT_TIME, eventTime)).setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = cassandraClient.getSession().execute(select);
    return results;
  }

  private static ResultSet findEventById(String eventId) {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.EVENTS).where(QueryBuilder.eq(AttributeConstants.EVENT_ID, eventId))
            .setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = cassandraClient.getSession().execute(select);
    return results;
  }

  public static void insertData(String key, String column, PreparedStatement preparedStatement) {
    try {
      BoundStatement boundStatement = new BoundStatement(preparedStatement);
      boundStatement.bind(key, column);
      (cassandraClient.getSession()).executeAsync(boundStatement);
    } catch (Exception e) {
      LOGGER.error("Inserting Data failed with exception: ", e);
    }
  }

  public static int postRequest(String inputEvent) throws Exception {
    HttpClient httpClient = new DefaultHttpClient();
    ;
    HttpResponse response = null;
    HttpPost postClient = new HttpPost(API_END_POINT);
    postClient.setHeader("Content-Type", "application/json");
    StringEntity entity = new StringEntity(inputEvent);
    entity.setContentType("application/json");
    postClient.setEntity(entity);
    response = httpClient.execute(postClient);
    return response.getStatusLine().getStatusCode();
  }
}
