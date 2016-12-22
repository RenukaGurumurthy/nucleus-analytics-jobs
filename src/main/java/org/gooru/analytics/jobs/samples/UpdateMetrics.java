package org.gooru.analytics.jobs.samples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.gooru.analytics.jobs.executor.EventMigration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class UpdateMetrics {

  private static Session session;
  private static PreparedStatement UPDATE_CLASS_ACTIVITY;
  private static final Logger LOG = LoggerFactory.getLogger(UpdateMetrics.class);

  public static void main(String args[]) {
    initializeComponent();
    UPDATE_CLASS_ACTIVITY = (session).prepare(
            "UPDATE students_class_activity SET time_spent = ? , views = ? WHERE class_uid = ? AND course_uid = ? AND unit_uid = ? AND lesson_uid =? AND collection_uid = ? AND collection_type = 'assessment' AND user_uid = ?");
    String dumpFile = "/home/ubuntu/class_score_to_be_updated.csv";
    processCsvAndUpdateMetrics(dumpFile);

  }

  private static void processCsvAndUpdateMetrics(String file) {
    BufferedReader fileReader = null;
    try {
      String line = "";
      fileReader = new BufferedReader(new FileReader(file));
      while ((line = fileReader.readLine()) != null) {
        // use comma as separator
        String[] row = line.split(",");
        String classId = row[0];
        String courseId = row[1];
        String unitId = row[2];
        String lessonId = row[3];
        String collectionId = row[4];
        String userId = row[5];

        ResultSet sessionIdSet = getSesstionIds(classId, courseId, unitId, lessonId, collectionId, userId);
        long timespent = 0, views = 0;
        for (Row sessionIdRow : sessionIdSet) {
          ResultSet metricsSet = getSessionWiseMetrics(sessionIdRow.getString("session_id"), collectionId);
          for (Row metricsRow : metricsSet) {
            timespent += metricsRow.getLong("time_spent");
            views += metricsRow.getLong("views");
          }
        }

        if (!update(classId, courseId, unitId, lessonId, collectionId, userId, timespent, views)) {
          LOG.info("classId:" + classId + "|courseId:" + courseId + "|unitId:" + unitId + "|lessonId:" + lessonId + "|collectionId:" + collectionId
                  + "|userId:" + userId + "|views:" + views + "|timespent:" + timespent);
          LOG.error("Exception while updating students_class_activity...");
        }

      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fileReader != null) {
        try {
          fileReader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static ResultSet getSesstionIds(String classId, String courseId, String unitId, String lessonId, String collectionId, String userId) {
    try {
      Statement select = QueryBuilder.select().all().from("event_logger_insights", "user_sessions").where(QueryBuilder.eq("user_uid", userId))
              .and(QueryBuilder.eq("collection_uid", collectionId)).and(QueryBuilder.eq("collection_type", "assessment"))
              .and(QueryBuilder.eq("class_uid", classId)).and(QueryBuilder.eq("course_uid", courseId)).and(QueryBuilder.eq("unit_uid", unitId))
              .and(QueryBuilder.eq("lesson_uid", lessonId));
      ResultSetFuture resultSetFuture = session.executeAsync(select);
      ResultSet result = resultSetFuture.get();
      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private static ResultSet getSessionWiseMetrics(String sessionId, String collectionId) {
    try {
      Statement select = QueryBuilder.select().all().from("event_logger_insights", "user_session_activity")
              .where(QueryBuilder.eq("session_id", sessionId)).and(QueryBuilder.eq("gooru_oid", collectionId));
      ResultSetFuture resultSetFuture = session.executeAsync(select);
      ResultSet result = resultSetFuture.get();
      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static boolean update(String classId, String courseId, String unitId, String lessonId, String collectionId, String userId, long timespent,
          long views) {
    try {
      BoundStatement boundStatement = new BoundStatement(UPDATE_CLASS_ACTIVITY);
      boundStatement.bind("time_spent", timespent);
      boundStatement.bind("views", views);
      boundStatement.bind("class_uid", classId);
      boundStatement.bind("course_uid", courseId);
      boundStatement.bind("unit_uid", unitId);
      boundStatement.bind("lesson_uid", lessonId);
      boundStatement.bind("collection_uid", collectionId);
      boundStatement.bind("user_uid", userId);
      (session).executeAsync(boundStatement);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  private static void initializeComponent() {
    Cluster cluster = Cluster.builder().withClusterName("AnalyticsClusterProdB").addContactPoints("10.2.40.100")
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    session = cluster.connect("event_logger_insights");

  }
}
