package org.gooru.analytics.jobs.infra;

import org.elasticsearch.client.Client;

import com.datastax.driver.core.Session;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

import io.vertx.core.json.JsonObject;

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

    private static final PostgreSQLConnection postgreSQLConnection = PostgreSQLConnection.instance();
    
    private static boolean connectionProviderStatus = true;
    
    
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
		postgreSQLConnection.initializeComponent(new JsonObject());
	}
	
	private static class CassandraClientHolder {
		public static final ConnectionProvider INSTANCE = new ConnectionProvider();
	}

	public static ConnectionProvider instance() {
		return CassandraClientHolder.INSTANCE;
	}

	public static boolean getConnectionProviderStatus(){
		return connectionProviderStatus;
	}
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<>(columnFamilyName, StringSerializer.get(),
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
