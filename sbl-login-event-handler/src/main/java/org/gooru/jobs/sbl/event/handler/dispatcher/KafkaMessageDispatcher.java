package org.gooru.jobs.sbl.event.handler.dispatcher;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.gooru.jobs.sbl.event.handler.components.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KafkaMessageDispatcher implements Dispatcher {
	private static final KafkaMessageDispatcher INSTANCE = new KafkaMessageDispatcher();
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageDispatcher.class);

	private static final Logger ERROR_LOGGER = LoggerFactory.getLogger("org.gooru.kafka.error.logs");

	private KafkaMessageDispatcher() {
	}

	public static KafkaMessageDispatcher getInstance() {
		return INSTANCE;
	}

	@Override
	public void dispatch(JsonObject eventBody) {

		LOGGER.info("preparing for message dispatch");
		Producer<String, String> producer = KafkaRegistry.getInstance().getKafkaProducer();
		ProducerRecord<String, String> kafkaMsg = new ProducerRecord<>(
				KafkaRegistry.getInstance().getKafkaProducerTopic(), eventBody.toString());

		try {
			LOGGER.debug("trying to produce message");
			if (producer != null) {
				LOGGER.debug("procuder is not null");
				producer.send(kafkaMsg, (metadata, exception) -> {
					if (exception == null) {
						LOGGER.info("Message Delivered Successfully: Offset : " + metadata.offset() + " : Topic : "
								+ metadata.topic() + " : Partition : " + metadata.partition() + " : Message : "
								+ kafkaMsg);
					} else {
						LOGGER.error("Fail to deliver message : {}", kafkaMsg, exception);
						ERROR_LOGGER.error(eventBody.toString());
					}
				});
			} else {
				LOGGER.error("Not able to obtain producer instance");
			}

		} catch (InterruptException | BufferExhaustedException | SerializationException ie) {
			// - If the thread is interrupted while blocked
			LOGGER.error("SendMesage2Kafka: to Kafka server:", ie);
		} 
	}

}
