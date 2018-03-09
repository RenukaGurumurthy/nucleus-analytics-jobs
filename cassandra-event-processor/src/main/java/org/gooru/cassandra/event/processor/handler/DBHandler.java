package org.gooru.cassandra.event.processor.handler;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.gooru.cassandra.event.processor.constants.EventConstants;
import org.gooru.cassandra.event.processor.entities.UserActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public class DBHandler implements EventHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBHandler.class);
	private final PreparedStatement insertStatement;
	private final PreparedStatement fetchStatement;
	private final PreparedStatement updateStatement;

	public DBHandler(PreparedStatement insertStatement, PreparedStatement fetchStatement,
			PreparedStatement updateStatment) {
		this.insertStatement = insertStatement;
		this.fetchStatement = fetchStatement;
		this.updateStatement = updateStatment;
	}

	@Override
	public void handle(JsonObject eventData) {
		String eventId = eventData.getString(EventConstants.EVENT_ID);
		String eventName = eventData.getString(EventConstants.EVENT_NAME);
		Long eventEndTime = eventData.getLong(EventConstants.EVENT_END_TIME);

		JsonObject payload = eventData.getJsonObject(EventConstants.PAYLOAD_OBJECT);
		JsonObject userData = (payload != null && !payload.isEmpty()) ? payload.getJsonObject(EventConstants.DATA)
				: null;

		if (userData == null || userData.isEmpty()) {
			LOGGER.warn("user data is not present in event, skipping");
			return;
		}

		LOGGER.debug("building user activity entity");
		UserActivity userActivity = new UserActivity(eventName, userData, eventEndTime);

		if (fetch(userActivity)) {
			update(userActivity);
		} else {
			if (insert(userActivity)) {
				LOGGER.debug("user activity saved sucessfully");
			} else {
				LOGGER.warn("unable to save user activity for eventId:{}", eventId);
			}
		}
	}

	private boolean fetch(UserActivity userActivity) {
		try {
			this.fetchStatement.setString(1, userActivity.getUserId());
			this.fetchStatement.setString(2, userActivity.getEventName());
			this.fetchStatement.setTimestamp(3, new Timestamp(userActivity.getEventEndTime()));

			ResultSet rs = this.fetchStatement.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			LOGGER.error("error while fetching user activity", e);

		}
		return false;
	}

	private boolean update(UserActivity userActivity) {
		LOGGER.debug("updating user activity");
		try {
			this.updateStatement.setString(1, userActivity.getUserId());
			this.updateStatement.setString(2, userActivity.getEventName());
			this.updateStatement.setString(3, userActivity.getLoginType());
			this.updateStatement.setString(4, userActivity.getTenantId());
			this.updateStatement.setString(5, userActivity.getUserCategory());
			this.updateStatement.setString(6, userActivity.getTenantRoot());
			this.updateStatement.setString(7, userActivity.getPartnerId());
			this.updateStatement.setTimestamp(8, new Timestamp(userActivity.getEventEndTime()));

			this.updateStatement.setString(9, userActivity.getUserId());
			this.updateStatement.setString(10, userActivity.getEventName());
			this.updateStatement.setTimestamp(11, new Timestamp(userActivity.getEventEndTime()));

			if (updateStatement.executeUpdate() != 1) {
				return false;
			}

			LOGGER.debug("record updated successfully saved..");
			return true;
		} catch (Exception e) {
			LOGGER.error("error while updating user activity", e);
			return false;
		}
	}

	private boolean insert(UserActivity userActivity) {
		LOGGER.debug("inserting user activity");
		try {
			this.insertStatement.setString(1, userActivity.getUserId());
			this.insertStatement.setString(2, userActivity.getEventName());
			this.insertStatement.setString(3, userActivity.getLoginType());
			this.insertStatement.setString(4, userActivity.getTenantId());
			this.insertStatement.setString(5, userActivity.getUserCategory());
			this.insertStatement.setString(6, userActivity.getTenantRoot());
			this.insertStatement.setString(7, userActivity.getPartnerId());
			this.insertStatement.setTimestamp(8, new Timestamp(userActivity.getEventEndTime()));

			if (insertStatement.executeUpdate() != 1) {
				return false;
			}

			LOGGER.debug("record inserted successfully..");
			return true;
		} catch (Exception e) {
			LOGGER.error("error while inserting user activity", e);
			return false;
		}
	}
}
