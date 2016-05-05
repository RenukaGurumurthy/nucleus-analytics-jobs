package org.gooru.migration.jobs;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import org.gooru.migration.connections.AnalyticsUsageCassandraClusterClient;
import org.gooru.migration.connections.PostgreSQLConnection;
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
	private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient
			.instance();
	private static final Logger LOG = LoggerFactory.getLogger(SyncTotalContentCounts.class);
	private static String GET_CLASS_COURSE = "select distinct class.id,col.course_id,class.updated_at from class class inner join collection col on col.course_id = class.course_id where class.updated_at > to_timestamp(?,'YYYY-MM-DD HH:MI:SS') - interval '3 minutes';";
	private static String GET_COURSE_COUNT = "select course_id as contentId, format, count(format) as totalCounts from collection where course_id = ? group by format,course_id;";
	private static String GET_UNIT_COUNT = "select unit_id as contentId, format, count(format) as totalCounts from collection where course_id = ? group by format,unit_id;";
	private static String GET_LESSON_COUNT = "select lesson_id as contentId, format, count(format) as totalCounts from collection where course_id = ? group by format,lesson_id;";
	private static String ID = "id";
	private static String COURSE_ID = "course_id";
	private static String currentTime = null;
	private static String UPDATED_AT = "updated_at";
	private static String CLASS_UID = "class_uid";
	private static String CONTENT_UID = "content_uid";
	private static String CONTENT_ID = "contentId";
	private static String CONTENT_TYPE = "content_type";
	private static String FORMAT = "format";
	private static String TOTAL_COUNT = "total_count";
	private static String TOTAL_COUNTS = "totalCounts";
	private static String CLASS_CONTENT_COUNT = "class_content_count";
	private static String SYNC_JOBS_PROPERTIES = "sync_jobs_properties";
	private static String _JOB_NAME = "job_name";
	private static String PROPERTY_NAME = "property_name";
	private static String PROPERTY_VALUE = "property_value";
	private static String LAST_UPDATED_TIME = "last_updated_time";
	private static SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public static void main(String args[]) {
		minuteDateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
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
				List<Map> classList = Base.findAll(GET_CLASS_COURSE, currentTime);
				String updatedTime = null;
				for (Map classCourseDetail : classList) {
					String classId = classCourseDetail.get(ID).toString();
					UUID courseId = UUID.fromString(classCourseDetail.get(COURSE_ID).toString());
					updatedTime = classCourseDetail.get(UPDATED_AT).toString();
					LOG.info("classId:" + classId + "-> courseId : " + courseId);
					List<Map> courseCount = Base.findAll(GET_COURSE_COUNT, courseId);
					List<Map> unitCount = Base.findAll(GET_UNIT_COUNT, courseId);
					List<Map> lessonCount = Base.findAll(GET_LESSON_COUNT, courseId);

					updateCounts(classId, courseCount);
					updateCounts(classId, unitCount);
					updateCounts(classId, lessonCount);
				}
				updateLastUpdatedTime(JOB_NAME, updatedTime == null ? currentTime : updatedTime);
				LOG.info("connection going to close.....................");
				Base.close();
			}
		};
		timer.scheduleAtFixedRate(task, 0, 120000);
	}

	private static void updateCounts(String classId, List<Map> collectionCount) {
		try {
			List<RegularStatement> stmtList = new ArrayList<RegularStatement>();
			RegularStatement[] arr = new RegularStatement[0];

			for (Map countDetails : collectionCount) {
				Insert insertStatmt = QueryBuilder
						.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(),
								CLASS_CONTENT_COUNT)
						.value(CLASS_UID, classId).value(CONTENT_UID, countDetails.get(CONTENT_ID).toString())
						.value(CONTENT_TYPE, countDetails.get(FORMAT).toString())
						.value(TOTAL_COUNT, ((Number) countDetails.get(TOTAL_COUNTS)).longValue());
				stmtList.add(insertStatmt);

			}
			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(QueryBuilder.batch(stmtList.toArray(arr)));
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while inserting data in class_content_count.{}", e);
		}

	}

	private static String getLastUpdatedTime() {
		try {
			Statement select = QueryBuilder.select().all()
					.from(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), SYNC_JOBS_PROPERTIES)
					.where(QueryBuilder.eq(_JOB_NAME, JOB_NAME)).and(QueryBuilder.eq(PROPERTY_NAME, LAST_UPDATED_TIME));
			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(select);
			ResultSet result = resultSetFuture.get();
			for (Row r : result) {
				return r.getString(PROPERTY_VALUE);
			}
		} catch (Exception e) {
			LOG.error("Error while reading job last updated time.{}", e);
		}
		return minuteDateFormatter.format(new Date());
	}

	private static void updateLastUpdatedTime(String jobName, String updatedTime) {
		try {
			Insert insertStatmt = QueryBuilder
					.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(), SYNC_JOBS_PROPERTIES)
					.value(_JOB_NAME, jobName).value(PROPERTY_NAME, LAST_UPDATED_TIME)
					.value(PROPERTY_VALUE, updatedTime);

			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(insertStatmt);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while updating last updated time.{}", e);
		}
	}
}
