package org.gooru.migration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import com.datastax.driver.core.Session;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public final class ConnectionProvider {
	private static Properties configConstants = new Properties();
	private static String analyticsCassKeyspace = null;
	private static String eventCassKeyspace = null;

	private static AnalyticsUsageCassandraClusterClient analyticsUsageCassandraClusterClient = null;
	private static EventCassandraClusterClient eventCassandraClusterClient = null;
	private static ArchivedCassandraClusterClient archivedCassandraClusterClient = null;
	private static KafkaConnectionProvider kafkaConnectionProvider = null;

	public ConnectionProvider() {
		loadConfigSettings();

		analyticsCassKeyspace = configConstants.getProperty("analytics.cassandra.keyspace", "event_logger_insights");
		eventCassKeyspace = configConstants.getProperty("event.cassandra.keyspace", "event_logger_insights");

		analyticsUsageCassandraClusterClient = new AnalyticsUsageCassandraClusterClient(
				configConstants.getProperty("analytics.cassandra.seeds", "127.0.0.1"),
				configConstants.getProperty("analytics.cassandra.datacenter", "datacenter1"),
				configConstants.getProperty("analytics.cassandra.cluster", "cassandra"),
				configConstants.getProperty("analytics.cassandra.keyspace", "event_logger_insights"));
		archivedCassandraClusterClient = new ArchivedCassandraClusterClient(
				configConstants.getProperty("archived.cassandra.seeds", "127.0.0.1"),
				configConstants.getProperty("archived.cassandra.datacenter", "datacenter1"),
				configConstants.getProperty("archived.cassandra.cluster", "cassandra"),
				configConstants.getProperty("archived.cassandra.keyspace", "event_logger_insights"));
		eventCassandraClusterClient = new EventCassandraClusterClient(
				configConstants.getProperty("event.cassandra.seeds", "127.0.0.1"),
				configConstants.getProperty("event.cassandra.datacenter", "datacenter1"),
				configConstants.getProperty("event.cassandra.cluster", "cassandra"),
				configConstants.getProperty("event.cassandra.keyspace", "event_logger_insights"));
		kafkaConnectionProvider = new KafkaConnectionProvider(
				configConstants.getProperty("kafka.brokers", "127.0.0.1:9092"));
	}

	public static void main(String args[]) {
		System.out.println("Connection established....");
		System.out.println("Working Directory = " + System.getProperty("user.dir"));

	}

	private void loadConfigSettings() {
		try {
			InputStream inputStream = null;
			String propFileName = "settings.properties";
			String configPath = System.getProperty("user.dir").concat("/");
			inputStream = FileUtils.openInputStream(new File(configPath.concat(propFileName)));
			configConstants.load(inputStream);
			inputStream.close();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
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

	public KafkaConnectionProvider getKafkaProducer() {
		return kafkaConnectionProvider;
	}
	public String getAnalyticsCassandraName() {
		return analyticsCassKeyspace;
	}

	public String getEventCassandraName() {
		return eventCassKeyspace;
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
		return configConstants.getProperty("metrics.publisher.topic", "test");
	}
}
