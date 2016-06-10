package org.gooru.analyics.jobs.executor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.gooru.analyics.jobs.constants.Constants;
import org.gooru.analyics.jobs.infra.AnalyticsUsageCassandraClusterClient;
import org.gooru.analyics.jobs.infra.ConfigSettingsLoader;
import org.gooru.analyics.jobs.infra.PostgreSQLConnection;
import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class SyncTotalContentCounts {
	private static final Timer timer = new Timer();
	private static final String JOB_NAME = "sync_total_content_counts";
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient
			.instance();
	private static final Logger LOG = LoggerFactory.getLogger(SyncTotalContentCounts.class);
	private static String currentTime = null;
    private static final SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final long JOB_INTERVAL = configSettingsLoader.getTotalCountsSyncInterval();
	
	public SyncTotalContentCounts() {
		LOG.info("deploying SyncTotalContentCounts....");
		minuteDateFormatter.setTimeZone(TimeZone.getTimeZone(Constants.UTC));
		final String jobLastUpdatedTime = getLastUpdatedTime();
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				PostgreSQLConnection.instance().intializeConnection();
				if (currentTime != null) {
					currentTime = minuteDateFormatter.format(new Date());
				} else {
					currentTime = jobLastUpdatedTime;
				}
				LOG.info("currentTime:" + currentTime);
				List<Map> classList = Base.findAll(Constants.GET_CLASS_COURSE, currentTime);
				String updatedTime = null;
				for (Map classCourseDetail : classList) {
					String classId = classCourseDetail.get(Constants.ID).toString();
					UUID courseId = UUID.fromString(classCourseDetail.get(Constants.COURSE_ID).toString());
					updatedTime = classCourseDetail.get(Constants.UPDATED_AT).toString();
					LOG.info("classId:" + classId + "-> courseId : " + courseId);
					List<Map> courseCount = Base.findAll(Constants.GET_COURSE_COUNT, courseId);
					List<Map> unitCount = Base.findAll(Constants.GET_UNIT_COUNT, courseId);
					List<Map> lessonCount = Base.findAll(Constants.GET_LESSON_COUNT, courseId);

					updateCounts(classId, courseCount);
					updateCounts(classId, unitCount);
					updateCounts(classId, lessonCount);
				}
				updateLastUpdatedTime(JOB_NAME, updatedTime == null ? currentTime : updatedTime);
				Base.close();
			}
		};
		timer.scheduleAtFixedRate(task, 0, JOB_INTERVAL);
	}

	private static void updateCounts(String classId, List<Map> collectionCount) {
		try {
			List<RegularStatement> stmtList = new ArrayList<>();
			RegularStatement[] arr = new RegularStatement[0];

			for (Map countDetails : collectionCount) {
				Insert insertStatmt = QueryBuilder
						.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(),
								Constants.CLASS_CONTENT_COUNT)
						.value(Constants.CLASS_UID, classId).value(Constants.CONTENT_UID, countDetails.get(Constants.CONTENT_ID).toString())
						.value(Constants.CONTENT_TYPE, countDetails.get(Constants.FORMAT).toString())
						.value(Constants.TOTAL_COUNT, ((Number) countDetails.get(Constants.TOTAL_COUNTS)).longValue());
				stmtList.add(insertStatmt);

			}
			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(QueryBuilder.batch(stmtList.toArray(arr)));
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while inserting data in class_content_count.", e);
		}

	}

	private static String getLastUpdatedTime() {
		try {
			Statement select = QueryBuilder.select().all()
					.from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
					.where(QueryBuilder.eq(Constants._JOB_NAME, JOB_NAME)).and(QueryBuilder.eq(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME));
			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(select);
			ResultSet result = resultSetFuture.get();
			for (Row r : result) {
				return r.getString(Constants.PROPERTY_VALUE);
			}
		} catch (Exception e) {
			LOG.error("Error while reading job last updated time.", e);
		}
		return minuteDateFormatter.format(new Date());
	}

	private static void updateLastUpdatedTime(String jobName, String updatedTime) {
		try {
			Insert insertStatmt = QueryBuilder
					.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), Constants.SYNC_JOBS_PROPERTIES)
					.value(Constants._JOB_NAME, jobName).value(Constants.PROPERTY_NAME, Constants.LAST_UPDATED_TIME)
					.value(Constants.PROPERTY_VALUE, updatedTime);

			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(insertStatmt);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while updating last updated time.", e);
		}
	}
}
