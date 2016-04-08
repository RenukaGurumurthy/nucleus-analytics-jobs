package org.gooru.migration.connections;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaClusterClient {

	private static Producer<String, String> producer = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();

	KafkaClusterClient() {
		initializeKafkaConnection(configSettingsLoader.getKakaBrokers());
	}

	private static class KafkaConnectionHolder {
		public static final KafkaClusterClient INSTANCE = new KafkaClusterClient();
	}

	public static KafkaClusterClient instance() {
		return KafkaConnectionHolder.INSTANCE;
	}

	private static void initializeKafkaConnection(String brokers) {
		Properties props = new Properties();
		props.put("metadata.broker.list", brokers);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		props.put("producer.type", "async");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	public Producer<String, String> getPublisher() {
		return producer;
	}
}
