package org.gooru.jobs.sbl.event.handler.components;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

public final class KafkaRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRegistry.class);
    private Producer<String, String> kafkaProducer;

    private String KAFKA_PRODUCER_TOPIC = null;

    private volatile boolean initialized = false;

    public void initializeComponent(JsonObject config) {
        // Skip if we are already initialized
        LOGGER.debug("Initialization called upon.");
        if (!initialized) {
            LOGGER.debug("May have to do initialization");
            // We need to do initialization, however, we are running it via
            // verticle instance which is going to run in multiple threads hence
            // we need to be safe for this operation
            synchronized (Holder.INSTANCE) {
                LOGGER.debug("Will initialize after double checking");
                if (!initialized) {
                    LOGGER.debug("Initializing KafkaRegistry now");
                    try {
                        initializeKafkaPublisher(config);
                        initialized = true;
                        LOGGER.debug("Initializing KafkaRegistry DONE");
                    } catch (Exception e) {
                        LOGGER.error("Initializing KafkaRegistry Failed :", e);
                    }
                }
            }
        }
    }

    private void initializeKafkaPublisher(JsonObject kafkaConfig) {
        LOGGER.debug("initializing kafka producer...");

        final Properties properties = new Properties();

        for (Map.Entry<String, Object> entry : kafkaConfig) {
            switch (entry.getKey()) {
            case ProducerConfig.BOOTSTRAP_SERVERS_CONFIG:
                properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("BOOTSTRAP_SERVERS_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.RETRIES_CONFIG:
                properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("RETRIES_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.BATCH_SIZE_CONFIG:
                properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("BATCH_SIZE_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.LINGER_MS_CONFIG:
                properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("LINGER_MS_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.BUFFER_MEMORY_CONFIG:
                properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("BUFFER_MEMORY_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG:
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("KEY_SERIALIZER_CLASS_CONFIG : " + entry.getValue());
                break;
            case ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG:
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("VALUE_SERIALIZER_CLASS_CONFIG : " + entry.getValue());
                break;
            case "producer.topic":
                this.KAFKA_PRODUCER_TOPIC = entry.getValue().toString();
                LOGGER.debug("KAFKA_PRODUCER_TOPIC : " + this.KAFKA_PRODUCER_TOPIC);
                break;
            case ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG:
                properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(entry.getValue()));
                LOGGER.debug("REQUEST_TIMEOUT_MS_CONFIG : " + entry.getValue());
                break;
            }
        }

        LOGGER.debug("kafka producer properties created...");
        this.kafkaProducer = new KafkaProducer<>(properties);

        LOGGER.debug("kafka producer initialized successfully!");
    }

    public Producer<String, String> getKafkaProducer() {
        if (initialized) {
            return this.kafkaProducer;
        }
        return null;
    }

    public void finalizeComponent() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
            this.kafkaProducer = null;
        }
    }

    public String getKafkaProducerTopic() {
        return this.KAFKA_PRODUCER_TOPIC;
    }

    public static KafkaRegistry getInstance() {
        return Holder.INSTANCE;
    }

    private KafkaRegistry() {
    }

    private static final class Holder {
        private static final KafkaRegistry INSTANCE = new KafkaRegistry();
    }
}
