package org.gooru.cassandra.event.processor.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.SerializationException;
import org.gooru.cassandra.event.processor.components.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MessageDispatcher {
    private static final MessageDispatcher INSTANCE = new MessageDispatcher();
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageDispatcher.class);

    private MessageDispatcher() {
    }

    public static MessageDispatcher getInstance() {
        return INSTANCE;
    }

    public void dispatch(JsonObject eventBody) {

        LOGGER.info("preparing for message dispatch");
        Producer<String, String> producer = KafkaRegistry.getInstance().getKafkaProducer();
        ProducerRecord<String, String> kafkaMsg =
            new ProducerRecord<>(KafkaRegistry.getInstance().getKafkaProducerTopic(), eventBody.toString());

        try {
            if (producer != null) {
                producer.send(kafkaMsg, (metadata, exception) -> {
                    if (exception == null) {
                        LOGGER.info("Message Delivered Successfully: Offset : " + metadata.offset() + " : Topic : "
                            + metadata.topic() + " : Partition : " + metadata.partition() + " : Message : " + kafkaMsg);
                    } else {
                        LOGGER.error("Fail to deliver message : {}", kafkaMsg, exception);
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
