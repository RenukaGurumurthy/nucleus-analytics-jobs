package org.gooru.migration.connections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public final class AnalyticsUsageCassandraClusterClient {

	private static Session session = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final Logger LOG = LoggerFactory.getLogger(AnalyticsUsageCassandraClusterClient.class);
	
	public AnalyticsUsageCassandraClusterClient() {
		initializeCluster();
	}

	private static void initializeCluster() {
		Cluster cluster = Cluster.builder().withClusterName(configSettingsLoader.getAnalyticsCassCluster())
				.addContactPoint(configSettingsLoader.getAnalyticsCassSeeds())
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
		session = cluster.connect(configSettingsLoader.getAnalyticsCassKeyspace());
		LOG.info("Analytics Cassandra initialized successfully...");
	}

	public Session getCassandraSession() {
		return session;
	}
	public String getAnalyticsCassKeyspace() {
		return configSettingsLoader.getAnalyticsCassKeyspace();
	}
	private static class AnalyticsUsageCassandraClusterClientHolder {
		public static final AnalyticsUsageCassandraClusterClient INSTANCE = new AnalyticsUsageCassandraClusterClient();
	}

	public static AnalyticsUsageCassandraClusterClient instance() {
		return AnalyticsUsageCassandraClusterClientHolder.INSTANCE;
	}
}
