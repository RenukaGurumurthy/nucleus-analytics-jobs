package org.gooru.migration.connections;

import org.elasticsearch.client.Client;

import com.datastax.driver.core.Session;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public final class ConnectionProvider {

	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();

	private static final AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = AnalyticsUsageCassandraClusterClient
			.instance();
	private static final EventCassandraClusterClient eventCassandraClusterClient = EventCassandraClusterClient
			.instance();
	private static final ArchivedCassandraClusterClient archivedCassandraClusterClient = ArchivedCassandraClusterClient
			.instance();
	private static final KafkaClusterClient kafkaConnectionProvider = KafkaClusterClient.instance();

	private static final ElasticsearchClusterClient elasticsearchClusterClient = ElasticsearchClusterClient.instance();

	private static PostgreSQLConnection postgreSQLConnection = PostgreSQLConnection.instance();

	public Client getSearchElsClient() {
		return elasticsearchClusterClient.getElsClient();
	}

	public Session getAnalyticsCassandraSession() {
		return analyticsUsageCassandraClusterClient.getCassandraSession();
	}

	public Session getEventCassandraSession() {
		return eventCassandraClusterClient.getCassandraSession();
	}

	public Keyspace getCassandraKeyspace() {
		return archivedCassandraClusterClient.getCassandraKeyspace();
	}

	public KafkaClusterClient getKafkaProducer() {
		return kafkaConnectionProvider;
	}

	public String getAnalyticsCassandraName() {
		return configSettingsLoader.getAnalyticsCassKeyspace();
	}

	public String getEventCassandraName() {
		return configSettingsLoader.getEventCassKeyspace();
	}

	public void openPSQLConnection(){
		postgreSQLConnection.intializeConnection();
	}
	
	private static class CassandraClientHolder {
		public static final ConnectionProvider INSTANCE = new ConnectionProvider();
	}

	public static ConnectionProvider instance() {
		return CassandraClientHolder.INSTANCE;
	}

	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(),
				StringSerializer.get());

		return aggregateColumnFamily;
	}

	public String getMetricsPublisherQueueTopic() {
		return configSettingsLoader.getStatPublisherTopic();
	}

	public ConfigSettingsLoader getConfigsettingsloader() {
		return configSettingsLoader;
	}
}
