package org.gooru.migration.connections;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaClusterClient {

	private static KafkaProducer<String, String> producer = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final Logger LOG = LoggerFactory.getLogger(KafkaClusterClient.class);
	KafkaClusterClient() {
		initializeKafkaConnection(configSettingsLoader.getKakaBrokers());
		LOG.info("Kafka Cluster initialized successfully..");
	}

	private static class KafkaConnectionHolder {
		public static final KafkaClusterClient INSTANCE = new KafkaClusterClient();
	}

	public static KafkaClusterClient instance() {
		return KafkaConnectionHolder.INSTANCE;
	}

	private static void initializeKafkaConnection(String brokers) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 0);
		props.put("block.on.buffer.full", true);
		props.put("auto.commit.interval.ms", 1000);
		producer = new KafkaProducer<>(props);
	}

	public KafkaProducer<String, String> getPublisher() {
		return producer;
	}
}
