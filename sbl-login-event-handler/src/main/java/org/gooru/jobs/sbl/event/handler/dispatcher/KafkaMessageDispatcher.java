package org.gooru.jobs.sbl.event.handler.dispatcher;

import io.vertx.core.json.JsonObject;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.gooru.jobs.sbl.event.handler.components.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaMessageDispatcher {
	private static final KafkaMessageDispatcher INSTANCE = new KafkaMessageDispatcher();
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageDispatcher.class);

	private static final Logger ERROR_LOGGER = LoggerFactory.getLogger("org.gooru.kafka.error.logs");

	private KafkaMessageDispatcher() {
	}

	public static KafkaMessageDispatcher getInstance() {
		return INSTANCE;
	}

	public Future<RecordMetadata> dispatch(JsonObject eventBody) {

		LOGGER.info("preparing for message dispatch");
		Producer<String, String> producer = KafkaRegistry.getInstance().getKafkaProducer();
		ProducerRecord<String, String> kafkaMsg = new ProducerRecord<>(
				KafkaRegistry.getInstance().getKafkaProducerTopic(), eventBody.toString());

		try {
			if (producer != null) {
				return producer.send(kafkaMsg);
			} else {
				LOGGER.error("Not able to obtain producer instance");
			}

		} catch (InterruptException | BufferExhaustedException | SerializationException ie) {
			// - If the thread is interrupted while blocked
			LOGGER.error("SendMesage2Kafka: to Kafka server:", ie);
		} 
		
		return null;
	}

}
