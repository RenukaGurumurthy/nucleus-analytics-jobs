package org.gooru.cassandra.event.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.gooru.cassandra.event.processor.components.DataSourceRegistry;
import org.gooru.cassandra.event.processor.constants.CassandraConstants;
import org.gooru.cassandra.event.processor.constants.EventConstants;
import org.gooru.cassandra.event.processor.handler.DBHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 26-Feb-2018
 */
public class CassandraEventProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEventProcessor.class);
	private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMddkkmm");

	private final Session session;

	public CassandraEventProcessor(Session session) {
		this.session = session;
	}

	public void process(Long startTime) {
		LOGGER.debug("inside process()");

		Long endTime = new Date().getTime();

		if (startTime >= endTime) {
			LOGGER.warn("start time is greater than endtime, nothing to process, aborting..");
			return;
		}

		Connection connection = null;
		try {
			connection = DataSourceRegistry.getInstance().getDataSource().getConnection();
			PreparedStatement insertStatement = connection.prepareStatement(
					"INSERT INTO user_activity(user_id, event_name, login_type, tenant_id, user_category, tenant_root, partner_id, updated_at)"
							+ " values(?,?,?,?,?,?,?,?)");
			PreparedStatement fetchStatement = connection.prepareStatement(
					"SELECT * FROM user_activity WHERE user_id = ? AND event_name = ? AND updated_at = ?");
			PreparedStatement updateStatement = connection.prepareStatement(
					"UPDATE user_activity SET user_id = ?, event_name = ?, login_type = ?, tenant_id = ?, user_category = ?, tenant_root = ?,"
							+ " partner_id = ?, updated_at = ? WHERE user_id = ? AND event_name = ? AND updated_at = ?");

			while (startTime < endTime) {
				String strStartTime = DATE_FORMATTER.format(new Date(startTime));

				try {
					ResultSet eventIds = findEventIdsByTime(strStartTime);

					if (eventIds != null) {
						for (Row eventId : eventIds) {
							LOGGER.debug("fetching eventId:{}", eventId.getString(CassandraConstants.EVENT_ID));
							Row eventInfo = null;
							try {
								eventInfo = findEventById(eventId.getString(CassandraConstants.EVENT_ID));

								if (eventInfo != null) {
									LOGGER.debug("event info for eventId:{} processing further", eventId);
									String eventData = eventInfo.getString(CassandraConstants.FIELDS);
									JsonObject eventJson = new JsonObject(eventData);
									if (isUserEvent(eventJson)) {
										LOGGER.debug("is user event, preparing to persist");
										new DBHandler(insertStatement, fetchStatement, updateStatement)
												.handle(eventJson);
									}
								}
							} catch (Exception e) {
								LOGGER.error("Unexpected exception while getting event. Can't do anything here..", e);
							}
						}
					} else {
						LOGGER.debug("no events found for time:{} ", strStartTime);
					}
				} catch (Exception e) {
					LOGGER.error("error while fetching events by time:{}", strStartTime, e);
				}

				startTime = new Date(startTime).getTime() + 60000;
			}
		} catch (Exception e) {
			LOGGER.error("error while saving user activity", e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	private boolean isUserEvent(JsonObject eventData) {
		return EventConstants.EVENTS.contains(eventData.getString(EventConstants.EVENT_NAME));
	}

	private ResultSet findEventIdsByTime(String eventTime) {
		Statement select = QueryBuilder.select().all().from(CassandraConstants.EVENTS_TIMELINE_INFO)
				.where(QueryBuilder.eq(CassandraConstants.EVENT_TIME, eventTime))
				.setConsistencyLevel(ConsistencyLevel.ONE);
		ResultSet results = this.session.execute(select);
		return results;
	}

	private Row findEventById(String eventId) {
		Statement select = QueryBuilder.select().all().from(CassandraConstants.EVENT_INFO)
				.where(QueryBuilder.eq(CassandraConstants.EVENT_ID, eventId)).setConsistencyLevel(ConsistencyLevel.ONE);
		ResultSet results = this.session.execute(select);
		return results.one();
	}
}
