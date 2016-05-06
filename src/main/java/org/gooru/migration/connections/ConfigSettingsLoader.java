package org.gooru.migration.connections;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConfigSettingsLoader {
	private static Properties configConstants = new Properties();
	private static final Logger LOG = LoggerFactory.getLogger(ConfigSettingsLoader.class);
	private static String analyticsCassKeyspace = null;
	private static String analyticsCassSeeds = null;
	private static String analyticsCassDatacenter = null;
	private static String analyticsCassCluster = null;

	private static String eventCassKeyspace = null;
	private static String eventCassSeeds = null;
	private static String eventCassDatacenter = null;
	private static String eventCassCluster = null;

	private static String archivedCassKeyspace = null;
	private static String archivedCassSeeds = null;
	private static String archivedCassDatacenter = null;
	private static String archivedCassCluster = null;

	private static String kafkaBrokers = null;

	private static int statMigrationQueueLimit = 0;
	private static long statMigrationInterval = 0L;
	private static int statPublisherQueueLimit = 0;
	private static long statPublisherInterval = 0L;
	private static String statPublisherTopic = null;

	private static String searchElsCluster = null;
	private static String searchElsHost = null;
	private static String searchIndexName = null;
	private static String searchTypeName = null;

	private static String plSqlUrl = null;
	private static String plSqlUserName = null;
	private static String plSqlPassword = null;

	ConfigSettingsLoader() {
		loadConfigSettings();

		analyticsCassSeeds = configConstants.getProperty("analytics.cassandra.seeds", "127.0.0.1");
		analyticsCassDatacenter = configConstants.getProperty("analytics.cassandra.datacenter", "datacenter1");
		analyticsCassCluster = configConstants.getProperty("analytics.cassandra.cluster", "cassandra");
		analyticsCassKeyspace = configConstants.getProperty("analytics.cassandra.keyspace", "event_logger_insights");

		archivedCassSeeds = configConstants.getProperty("archived.cassandra.seeds", "127.0.0.1");
		archivedCassDatacenter = configConstants.getProperty("archived.cassandra.datacenter", "datacenter1");
		archivedCassCluster = configConstants.getProperty("archived.cassandra.cluster", "cassandra");
		archivedCassKeyspace = configConstants.getProperty("archived.cassandra.keyspace", "event_logger_insights");

		eventCassSeeds = configConstants.getProperty("event.cassandra.seeds", "127.0.0.1");
		eventCassDatacenter = configConstants.getProperty("event.cassandra.datacenter", "datacenter1");
		eventCassCluster = configConstants.getProperty("event.cassandra.cluster", "cassandra");
		eventCassKeyspace = configConstants.getProperty("event.cassandra.keyspace", "event_logger_insights");

		kafkaBrokers = configConstants.getProperty("kafka.brokers", "127.0.0.1:9092");

		statMigrationQueueLimit = Integer.parseInt((String) configConstants.get("stat.migration.queue.limit"));
		statMigrationInterval = Long.parseLong((String) configConstants.get("stat.migration.delay"));
		statPublisherQueueLimit = Integer.parseInt((String) configConstants.get("stat.publisher.queue.limit"));
		statPublisherInterval = Long.parseLong((String) configConstants.get("stat.publisher.delay"));
		statPublisherTopic = (String) configConstants.get("metrics.publisher.topic");

		searchElsCluster = (String) configConstants.get("search.elasticsearch.cluster");
		searchElsHost = (String) configConstants.get("search.elasticsearch.ip");
		searchIndexName = (String) configConstants.get("search.elasticsearch.index");
		searchTypeName = (String) configConstants.get("search.elasticsearch.type");

		plSqlUrl = (String) configConstants.get("postgresql.driverurl");
		plSqlUserName = (String) configConstants.get("postgresql.username");
		plSqlPassword = (String) configConstants.get("postgresql.password");

	}

	private static class ConfigSettingsHolder {
		public static final ConfigSettingsLoader INSTANCE = new ConfigSettingsLoader();
	}

	public static ConfigSettingsLoader instance() {
		return ConfigSettingsHolder.INSTANCE;
	}

	private void loadConfigSettings() {
		try {
			InputStream inputStream = null;
			String propFileName = "settings.properties";
			String configPath = System.getProperty("user.dir").concat("/");
			inputStream = FileUtils.openInputStream(new File(configPath.concat(propFileName)));
			configConstants.load(inputStream);
			inputStream.close();
		} catch (Exception e) {
			LOG.error("Error while loading config properties.", e);
		}
	}

	public Properties getConfigConstants() {
		return configConstants;
	}

	public String getAnalyticsCassKeyspace() {
		return analyticsCassKeyspace;
	}

	public String getAnalyticsCassSeeds() {
		return analyticsCassSeeds;
	}

	public String getAnalyticsCassDatacenter() {
		return analyticsCassDatacenter;
	}

	public String getAnalyticsCassCluster() {
		return analyticsCassCluster;
	}

	public String getEventCassKeyspace() {
		return eventCassKeyspace;
	}

	public String getEventCassSeeds() {
		return eventCassSeeds;
	}

	public String getEventCassDatacenter() {
		return eventCassDatacenter;
	}

	public String getEventCassCluster() {
		return eventCassCluster;
	}

	public String getArchivedCassKeyspace() {
		return archivedCassKeyspace;
	}

	public String getArchivedCassSeeds() {
		return archivedCassSeeds;
	}

	public String getArchivedCassDatacenter() {
		return archivedCassDatacenter;
	}

	public String getArchivedCassCluster() {
		return archivedCassCluster;
	}

	public String getKakaBrokers() {
		return kafkaBrokers;
	}

	public int getStatMigrationQueueLimit() {
		return statMigrationQueueLimit;
	}

	public long getStatMigrationInterval() {
		return statMigrationInterval;
	}

	public int getStatPublisherQueueLimit() {
		return statPublisherQueueLimit;
	}

	public long getStatPublisherInterval() {
		return statPublisherInterval;
	}

	public String getStatPublisherTopic() {
		return statPublisherTopic;
	}

	public String getSearchElsCluster() {
		return searchElsCluster;
	}

	public String getSearchElsHost() {
		return searchElsHost;
	}

	public String getSearchElsIndex() {
		return searchIndexName;
	}
	public String getSearchElsType() {
		return searchTypeName;
	}
	public String getPlSqlUrl() {
		return plSqlUrl;
	}

	public String getPlSqlUserName() {
		return plSqlUserName;
	}

	public String getPlSqlPassword() {
		return plSqlPassword;
	}
}
