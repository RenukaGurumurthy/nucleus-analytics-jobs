package org.gooru.nucleus.replay.jobs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;

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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class ReplayEvents extends TimerTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobInitializer.class);
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
  private CassandraClient cassandraClient;
  private String API_END_POINT;
  private Long cutOffTimeInMs;
  private static long JOB_ID = 1L;

  public ReplayEvents(CassandraClient cassandraClient, String logAPIEndPoint, Long cutOffTime) {
    this.cassandraClient = cassandraClient;
    this.API_END_POINT = logAPIEndPoint;
    this.cutOffTimeInMs = cutOffTime;
  }

  @Override
  public void run() {
    try {
      Long startTime = null;
      Long endTime = null;
      String startEventTime = null;
      String endEventTime = null;
      String jobStatus = null;
      ResultSet jobStatusResultSet = getJobStatus();
      if (jobStatusResultSet != null) {
        for (Row jobStatusRow : jobStatusResultSet) {
          jobStatus = jobStatusRow.getString(AttributeConstants.PROPERTY_VALUE);
        }
      }
      if (jobStatus != null && jobStatus.equalsIgnoreCase(AttributeConstants.PROP_INPROGRESS)) {
        LOGGER.warn("Previous job is still running. Halting JOB : " + JOB_ID);
      } else {
        saveJobStatus(AttributeConstants.PROP_INPROGRESS);
        ResultSet startTimeResultSet = findEventStartTime();
        if (startTimeResultSet != null) {
          for (Row eventTime : startTimeResultSet) {
            startEventTime = eventTime.getString(AttributeConstants.PROPERTY_VALUE);
          }
        }
        if (startEventTime == null) {
          System.out.println("Can't execute job without start time!. Halting job!!");
          System.exit(0);
        } else {
          startTime = minuteDateFormatter.parse(startEventTime).getTime();
          // Set endTime..
          endTime = (new Date().getTime() - this.cutOffTimeInMs);
          LOGGER.info("startTime : " + startEventTime);
          endEventTime = minuteDateFormatter.format(new Date(endTime));
          LOGGER.info("endTime : " + endEventTime);
        }
        if (endTime < startTime) {
          LOGGER.info("Starting JOB : " + JOB_ID);
          for (Long startDate = startTime; startDate < endTime;) {
            String currentDate = minuteDateFormatter.format(new Date(startDate));
            LOGGER.info("Running for :" + currentDate);
            ResultSet eventIds = null;
            try {
              eventIds = findEventIdsByTime(currentDate);
            } catch (Exception e) {
              // FIXME:Today we can skip this exception. We need to look at it
              // in
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
                    LOGGER.info("Status:" + status + " for eventID : " + new JSONObject(eventData).getString("eventId"));
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
          saveLastProcessedTime(endEventTime);
          LOGGER.info("DONE!! Closing JOB: " + JOB_ID);
          saveJobStatus(AttributeConstants.PROP_COMPLETED);
          JOB_ID++;
        } else {
          LOGGER.warn("startTime should not be greater than endTime!");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private ResultSet findEventIdsByTime(String eventTime) {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.EVENTS_TIMELINE)
            .where(QueryBuilder.eq(AttributeConstants.EVENT_TIME, eventTime)).setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = this.cassandraClient.getSession().execute(select);
    return results;
  }

  private ResultSet findEventById(String eventId) {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.EVENTS).where(QueryBuilder.eq(AttributeConstants.EVENT_ID, eventId))
            .setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = this.cassandraClient.getSession().execute(select);
    return results;
  }

  private ResultSet findEventStartTime() {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.CONFIG_PROPERTY)
            .where(QueryBuilder.eq(AttributeConstants.PROPERTY_NAME, AttributeConstants.PROP_START_TIME)).setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = this.cassandraClient.getSession().execute(select);
    return results;
  }

  private ResultSet getJobStatus() {
    Statement select = QueryBuilder.select().all().from(ConfigConstants.CONFIG_PROPERTY)
            .where(QueryBuilder.eq(AttributeConstants.PROPERTY_NAME, AttributeConstants.PROP_EVENT_REPLAY_JOB_STATUS))
            .setConsistencyLevel(ConsistencyLevel.ONE);
    ResultSet results = this.cassandraClient.getSession().execute(select);
    return results;
  }

  private void saveLastProcessedTime(String propValue) {
    Statement insert = QueryBuilder.insertInto(this.cassandraClient.getSession().getLoggedKeyspace(), ConfigConstants.CONFIG_PROPERTY)
            .value(AttributeConstants.PROPERTY_NAME, AttributeConstants.PROP_START_TIME).value(AttributeConstants.PROPERTY_VALUE, propValue)
            .setConsistencyLevel(ConsistencyLevel.ONE);
    this.cassandraClient.getSession().execute(insert);
  }

  private void saveJobStatus(String propValue) {
    Statement insert = QueryBuilder.insertInto(this.cassandraClient.getSession().getLoggedKeyspace(), ConfigConstants.CONFIG_PROPERTY)
            .value(AttributeConstants.PROPERTY_NAME, AttributeConstants.PROP_EVENT_REPLAY_JOB_STATUS)
            .value(AttributeConstants.PROPERTY_VALUE, propValue).setConsistencyLevel(ConsistencyLevel.ONE);
    this.cassandraClient.getSession().execute(insert);
  }

  public int postRequest(String inputEvent) throws Exception {
    HttpClient httpClient = new DefaultHttpClient();
    ;
    HttpResponse response = null;
    HttpPost postClient = new HttpPost(this.API_END_POINT);
    postClient.setHeader("Content-Type", "application/json");
    StringEntity entity = new StringEntity(inputEvent);
    entity.setContentType("application/json");
    postClient.setEntity(entity);
    response = httpClient.execute(postClient);
    return response.getStatusLine().getStatusCode();
  }
}
