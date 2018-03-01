/**
 * 
 */
package org.gooru.jobs.sbl.event.processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.gooru.jobs.sbl.event.handler.components.DataSourceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author gooru
 *
 */
public final class DBHelper {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBHelper.class);

	private DBHelper() {
		throw new AssertionError();
	}

	public static JsonObject getUserData(String userId) throws SQLException {
		Connection connection = null;
		JsonObject user = new JsonObject();

		try {
			LOGGER.debug("fetching user details:{}", userId);
			connection = DataSourceRegistry.getInstance().getDataSource().getConnection();
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM users WHERE id = '" + userId + "'::uuid");

			if (rs.next()) {
				user.put("tenant_id", rs.getString("tenant_id"));
				user.put("country", rs.getString("country"));
				user.put("school_district", rs.getString("school_district"));
				user.put("reference_id", rs.getString("reference_id"));
				user.put("gender", rs.getString("gender"));
				user.put("login_type", rs.getString("login_type"));
				user.put("birth_date", rs.getString("birth_date"));
				user.put("last_name", rs.getString("last_name"));
				user.put("tenant_root", rs.getString("tenant_root"));
				user.put("user_category", rs.getString("user_category"));
				user.put("school_id", rs.getString("school_id"));
				user.put("school", rs.getString("school"));
				user.put("school_district_id", rs.getString("school_district_id"));
				user.put("id", rs.getString("id"));
				user.put("state", rs.getString("state"));
				user.put("state_id", rs.getString("state_id"));
				user.put("first_name", rs.getString("first_name"));
				user.put("email", rs.getString("email"));
				user.put("country_id", rs.getString("country_id"));
				user.put("username", rs.getString("username"));
			}

		} catch (SQLException e) {
			LOGGER.error("exception while getting user '{}' from database", userId, e);
			throw e;
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error("unable to close connection", e);
			}
		}

		return user;
	}
}
