package org.gooru.migration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public final class AnalyticsUsageCassandraClusterClient {

	private static Session session = null;

	AnalyticsUsageCassandraClusterClient(String host, String datacenter, String clusterName, String keyspaceName) {
		initializeCluster(host, datacenter, clusterName, keyspaceName);
	}

	public AnalyticsUsageCassandraClusterClient() {
		this(null, null, null, null);
	}

	private static void initializeCluster(String host, String datacenter, String clusterName, String keyspaceName) {
		Cluster cluster = Cluster.builder().withClusterName(clusterName).addContactPoint(host)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000))
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(datacenter))).build();
		session = cluster.connect(keyspaceName);
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
