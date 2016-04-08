package org.gooru.migration.connections;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public final class AnalyticsUsageCassandraClusterClient {

	private static Session session = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();

	public AnalyticsUsageCassandraClusterClient() {
		initializeCluster();
	}

	private static void initializeCluster() {
		Cluster cluster = Cluster.builder().withClusterName(configSettingsLoader.getAnalyticsCassCluster())
				.addContactPoint(configSettingsLoader.getAnalyticsCassSeeds())
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000))
				.withLoadBalancingPolicy(new TokenAwarePolicy(
						new DCAwareRoundRobinPolicy(configSettingsLoader.getAnalyticsCassDatacenter())))
				.build();
		session = cluster.connect(configSettingsLoader.getAnalyticsCassKeyspace());
	}

	public Session getCassandraSession() {
		return session;
	}

	private static class AnalyticsUsageCassandraClusterClientHolder {
		public static final AnalyticsUsageCassandraClusterClient INSTANCE = new AnalyticsUsageCassandraClusterClient();
	}

	public static AnalyticsUsageCassandraClusterClient instance() {
		return AnalyticsUsageCassandraClusterClientHolder.INSTANCE;
	}
}
