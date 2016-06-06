package org.gooru.analyics.jobs.infra;

import org.json.JSONObject;

public final class ConfigSettingsLoader {
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

	private static long syncClassMembersInterval = 0L;
	private static long syncContentAuthrizersInterval = 0L;
	private static long syncTotalCountsInterval = 0L;
	
	ConfigSettingsLoader(JSONObject config) {

		analyticsCassSeeds = config.getString("analytics.cassandra.seeds");
		analyticsCassDatacenter = config.getString("analytics.cassandra.datacenter");
		analyticsCassCluster = config.getString("analytics.cassandra.cluster");
		analyticsCassKeyspace = config.getString("analytics.cassandra.keyspace");

		archivedCassSeeds = config.getString("archived.cassandra.seeds");
		archivedCassDatacenter = config.getString("archived.cassandra.datacenter");
		archivedCassCluster = config.getString("archived.cassandra.cluster");
		archivedCassKeyspace = config.getString("archived.cassandra.keyspace");

		eventCassSeeds = config.getString("event.cassandra.seeds");
		eventCassDatacenter = config.getString("event.cassandra.datacenter");
		eventCassCluster = config.getString("event.cassandra.cluster");
		eventCassKeyspace = config.getString("event.cassandra.keyspace");

		kafkaBrokers = config.getString("kafka.brokers");

		statMigrationQueueLimit = config.getInt("stat.migration.queue.limit");
		statMigrationInterval = config.getLong("stat.migration.delay");
		statPublisherQueueLimit = config.getInt("stat.publisher.queue.limit");
		statPublisherInterval = config.getInt("stat.publisher.delay");
		statPublisherTopic = config.getString("metrics.publisher.topic");

		searchElsCluster = config.getString("search.elasticsearch.cluster");
		searchElsHost = config.getString("search.elasticsearch.ip");
		searchIndexName = config.getString("search.elasticsearch.index");
		searchTypeName = config.getString("search.elasticsearch.type");

		plSqlUrl = config.getString("postgresql.driverurl");
		plSqlUserName = config.getString("postgresql.username");
		plSqlPassword = config.getString("postgresql.password");

		syncClassMembersInterval = config.getLong("class.members.sync.delay");
		syncContentAuthrizersInterval = config.getLong("content.authorizers.sync.delay");
		syncTotalCountsInterval =config.getLong("total.counts.sync.delay");
		
	}
	ConfigSettingsLoader() {
		
	}

	private static class ConfigSettingsHolder {
		public static final ConfigSettingsLoader INSTANCE = new ConfigSettingsLoader();
	}

	public static ConfigSettingsLoader instance() {
		return ConfigSettingsHolder.INSTANCE;
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
	public long getClassMembersSyncInterval() {
		return syncClassMembersInterval;
	}
	public long getTotalCountsSyncInterval() {
		return syncTotalCountsInterval;
	}
	public long getContentAuthorizersSyncInterval() {
		return syncContentAuthrizersInterval;
	}
}
