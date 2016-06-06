package org.gooru.analyics.jobs.infra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public final class EventCassandraClusterClient {

	private static Session session = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final Logger LOG = LoggerFactory.getLogger(EventCassandraClusterClient.class);
	
	EventCassandraClusterClient() {
		initializeCluster(configSettingsLoader.getEventCassSeeds(), configSettingsLoader.getEventCassDatacenter(),
				configSettingsLoader.getEventCassCluster(), configSettingsLoader.getEventCassKeyspace());
	}

	private static void initializeCluster(String host, String datacenter, String clusterName, String keyspaceName) {
		Cluster cluster = Cluster.builder().withClusterName(clusterName).addContactPoint(host)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
		session = cluster.connect(keyspaceName);
		LOG.info("Event Cassandra Cluster Initialized successfully..");
	}

	public Session getCassandraSession() {
		return session;
	}

	public String getAnalyticsCassKeyspace() {
		return configSettingsLoader.getEventCassKeyspace();
	}
	
	private static class EventCassandraClusterClientHolder {
		public static final EventCassandraClusterClient INSTANCE = new EventCassandraClusterClient();
	}

	public static EventCassandraClusterClient instance() {
		return EventCassandraClusterClientHolder.INSTANCE;
	}
}
