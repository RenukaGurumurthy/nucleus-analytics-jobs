package org.gooru.jobs.sbl.event.processor;

import java.sql.SQLException;
import java.util.Base64;
import java.util.UUID;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 28-Feb-2018
 */
public class EventBodyBuilder {

	private final JsonObject event;

	public EventBodyBuilder(JsonObject event) {
		this.event = event;
	}

	public JsonObject build() {
		JsonObject eventBody = new JsonObject();
		String userId = this.event.getJsonObject("event.body").getString("id");
		String sessionToken = this.event.getString("session.token");

		long eventTime = getTokenTime(sessionToken);
		eventBody.put("startTime", eventTime);
		eventBody.put("endTime", eventTime);
		eventBody.put("eventId", UUID.randomUUID().toString());
		eventBody.put("eventName", "event.user.signin");

		JsonObject session = new JsonObject();
		session.putNull("apiKey");
		session.put("sessionToken", sessionToken);
		session.putNull("organizationUId");
		eventBody.put("session", session);

		JsonObject user = new JsonObject();
		user.putNull("userIp");
		user.putNull("userAgent");
		user.put("gooruUId", userId);
		eventBody.put("user", user);

		JsonObject version = new JsonObject();
		version.put("logApi", "4.0");
		eventBody.put("version", version);

		JsonObject context = new JsonObject();
		context.put("clientSource", "auth");
		eventBody.put("context", context);

		JsonObject payloadObject = new JsonObject();

		try {
			payloadObject.put("data", DBHelper.getUserData(userId));
		} catch (SQLException e) {
			return null;
		}

		payloadObject.put("subEventName", "event.user.signin");

		eventBody.put("payLoadObject", payloadObject);

		return eventBody;
	}

	private long getTokenTime(String token) {
		String decodedToken = new String(Base64.getDecoder().decode(token));
		return new Long(decodedToken.split(":")[1]);
	}
}
