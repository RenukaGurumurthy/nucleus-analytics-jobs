package org.gooru.migration.connections;

import org.javalite.activejdbc.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLConnection {
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final Logger LOG = LoggerFactory.getLogger(PostgreSQLConnection.class);
	PostgreSQLConnection() {
		intializeConnection();
	}

	private static class PostgreSQLConnectionHolder {
		public static final PostgreSQLConnection INSTANCE = new PostgreSQLConnection();
	}

	public static PostgreSQLConnection instance() {
		return PostgreSQLConnectionHolder.INSTANCE;
	}

	public void intializeConnection() {
		try {
			if (!Base.hasConnection()) {
				Base.open("org.postgresql.Driver", configSettingsLoader.getPlSqlUrl(),
						configSettingsLoader.getPlSqlUserName(), configSettingsLoader.getPlSqlPassword());
			}
			Base.openTransaction();
		} catch (Exception e) {
			LOG.error("Error while initializing postgreSQL....{}",e);
		} 
	}

}
