package org.gooru.migration.jobs;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.gooru.migration.connections.AnalyticsUsageCassandraClusterClient;
import org.gooru.migration.connections.PostgreSQLConnection;
import org.javalite.activejdbc.Base;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class SyncContentAuthorizedUsers {
	private static final Timer timer = new Timer();
	private static final String JOB_NAME = "sync_content_authorized_users";
	private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient
			.instance();
	private static final Logger LOG = LoggerFactory.getLogger(SyncContentAuthorizedUsers.class);
	private static final String GET_AUTHORIZED_USERS_QUERY = "select id,creator_id,collaborator,updated_at from class where class.updated_at > to_timestamp(?,'YYYY-MM-DD HH:MI:SS') - interval '3 minutes';";
	private static String ID = "id";
	private static String CREATOR_ID = "creator_id";
	private static String CREATOR_UID = "creator_uid";
	private static String GOORU_OID = "gooru_oid";
	private static String COLLABORATOR = "collaborator";
	private static String COLLABORATORS = "collaborators";
	private static String currentTime = null;
	private static String UPDATED_AT = "updated_at";
	private static String SYNC_JOBS_PROPERTIES = "sync_jobs_properties";
	private static String _JOB_NAME = "job_name";
	private static String PROPERTY_NAME = "property_name";
	private static String PROPERTY_VALUE = "property_value";
	private static String LAST_UPDATED_TIME = "last_updated_time";
	private static String CONTENT_AUTHORIZED_USERS = "content_authorized_users";
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
				String updatedTime = null;
				List<Map> classList = Base.findAll(GET_AUTHORIZED_USERS_QUERY, currentTime);
				for (Map classCourseDetail : classList) {
					String classId = classCourseDetail.get(ID).toString();
					String creator = classCourseDetail.get(CREATOR_ID).toString();
					Object collabs = classCourseDetail.get(COLLABORATOR);
					updatedTime = classCourseDetail.get(UPDATED_AT).toString();
					HashSet<String> collaborators = null;
					LOG.info("class : " + classId);
					if (collabs != null) {
						JSONArray collabsArray = new JSONArray(collabs.toString());
						collaborators = new HashSet<String>();
						for (int index = 0; index < collabsArray.length(); index++) {
							collaborators.add(collabsArray.getString(index));
						}
					}
					updateAuthorizedUsers(classId, creator, collaborators);
				}
				updateLastUpdatedTime(JOB_NAME, updatedTime == null ? currentTime : updatedTime);
				Base.close();
			}
		};
		timer.scheduleAtFixedRate(task, 0, 120000);
	}

	private static void updateAuthorizedUsers(String classId, String creator, HashSet<String> collabs) {
		try {
			Insert insert = QueryBuilder
					.insertInto(analyticsUsageCassandraClusterClient.getAnalyticsCassKeyspace(),
							CONTENT_AUTHORIZED_USERS)
					.value(GOORU_OID, classId).value(COLLABORATORS, collabs).value(CREATOR_UID, creator);

			ResultSetFuture resultSetFuture = analyticsUsageCassandraClusterClient.getCassandraSession()
					.executeAsync(insert);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while updating authorized user details.{}", e);
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
