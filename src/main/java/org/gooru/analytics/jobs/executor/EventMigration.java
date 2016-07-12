package org.gooru.analytics.jobs.executor;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.gooru.analytics.jobs.infra.EventCassandraClusterClient;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.ConstantBackoff;

import io.vertx.core.json.JsonObject;

public class EventMigration implements JobInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(EventMigration.class);
  private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
  private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient.instance();
  private static final ArchivedCassandraClusterClient archivedCassandraClusterClient = ArchivedCassandraClusterClient.instance();
  private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient.instance();
  private static final String JOB_NAME = "event_migration";
  private static final PreparedStatement insertEvents =
          (eventCassandraClusterClient.getCassandraSession()).prepare("INSERT INTO events(event_id,fields)VALUES(?,?)");
  private static final PreparedStatement insertEventTimeLine =
          (eventCassandraClusterClient.getCassandraSession()).prepare("INSERT INTO events_timeline(event_time,event_id)VALUES(?,?);");

  private static class EventMigrationHolder {
    public static final EventMigration INSTANCE = new EventMigration();
  }

  public static EventMigration instance() {
    return EventMigrationHolder.INSTANCE;
  }

  public void deployJob(JsonObject config) {

    LOG.info("deploying EventMigration....");
    try {
      String status = getJobStatus();
      if (!Constants.STOP.equalsIgnoreCase(status)) {
        String start = getLastUpdatedTime();

        Long startTime = minuteDateFormatter.parse(start).getTime();
        LOG.info("startTime : " + start);
        Long endTime = new Date().getTime();
        LOG.info("endTime : " + endTime);
        for (Long startDate = startTime; startDate < endTime;) {
          String currentDate = minuteDateFormatter.format(new Date(startDate));
          LOG.info("Running for :" + currentDate);
          // Incrementing time - one minute
          long s = System.currentTimeMillis();
          ColumnList<String> et = readWithKey(Constants.EVENT_TIMIELINE, currentDate);
          long st = System.currentTimeMillis();
          LOG.info("time to get data from event_timeline: " + (st-s));
          for (String eventId : et.getColumnNames()) {
            LOG.info("eventId: " + eventId);
            long s1 = System.currentTimeMillis();
            ColumnList<String> ef = readWithKey(Constants.EVENT_DETAIL, eventId);
            long s2 = System.currentTimeMillis();
            LOG.info("time to get data from event_detail: " + (s2-s1));
            // Insert event_time_line
            insertData(currentDate, et.getStringValue(eventId, Constants.NA), insertEventTimeLine);
            // Insert events
            insertData(eventId, ef.getStringValue(Constants.FIELDS, Constants.NA), insertEvents);
            long s3 = System.currentTimeMillis();
            LOG.info("time to write: " + (s3-s2));
          }
          startDate = new Date(startDate).getTime() + 60000;
          updateLastUpdatedTime(JOB_NAME, currentDate);
        }
      } else {
        LOG.info("Event migration job stopped!!");
      }
    } catch (Exception e) {
      if (e instanceof ArrayIndexOutOfBoundsException) {
        LOG.info("startTime or endTime can not be null. Please make sure the class execution format as below.");
        LOG.info("java -classpath build/libs/migration-scripts-fat.jar: org.gooru.migration.jobs.EventMigration 201508251405 201508251410");
      } else {
        LOG.error("Something went wrong...", e);
      }
      System.exit(500);
    }

  }

  public static ColumnList<String> readWithKey(String cfName, String key) {

    ColumnList<String> result = null;
    try {
      result = (archivedCassandraClusterClient.getCassandraKeyspace()).prepareQuery(archivedCassandraClusterClient.accessColumnFamily(cfName))
              .setConsistencyLevel(ConsistencyLevel.CL_LOCAL_QUORUM).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute().getResult();

    } catch (Exception e) {
      LOG.error("Failure in reading with key", e);
    }

    return result;
  }

  public static void insertData(String key, String column, PreparedStatement preparedStatement) {
    try {
      BoundStatement boundStatement = new BoundStatement(preparedStatement);
      boundStatement.bind(key, column);
      (eventCassandraClusterClient.getCassandraSession()).executeAsync(boundStatement);
    } catch (Exception e) {
      LOG.error("Inserting Data failed with exception: ", e);
    }
  }

  private static String getLastUpdatedTime() {
    try {
      Statement select = QueryBuilder.select().all()
              .from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
              .where(QueryBuilder.eq(Constants._JOB_NAME, JOB_NAME)).and(QueryBuilder.eq(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME)).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(select);
      ResultSet result = resultSetFuture.get();
      for (Row r : result) {
        return r.getString(Constants.PROPERTY_VALUE);
      }
    } catch (Exception e) {
      LOG.error("Error while reading job last updated time.{}", e);
    }
    return minuteDateFormatter.format(new Date());
  }

  private static String getJobStatus() {
    try {
      Statement select =
              QueryBuilder.select().all().from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
                      .where(QueryBuilder.eq(Constants._JOB_NAME, JOB_NAME)).and(QueryBuilder.eq(Constants.PROPERTY_NAME, Constants.STATUS)).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);
      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(select);
      ResultSet result = resultSetFuture.get();
      for (Row r : result) {
        return r.getString(Constants.PROPERTY_VALUE);
      }
    } catch (Exception e) {
      LOG.error("Error while reading job last updated time.{}", e);
    }
    return Constants.STOP;
  }

  private static void updateLastUpdatedTime(String jobName, String updatedTime) {
    try {
      Statement insertStatmt = QueryBuilder.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
              .value(Constants._JOB_NAME, jobName).value(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME)
              .value(Constants.PROPERTY_VALUE, updatedTime).setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);

      ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession().executeAsync(insertStatmt);
      resultSetFuture.get();
    } catch (Exception e) {
      LOG.error("Error while updating last updated time.", e);
    }
  }
}
