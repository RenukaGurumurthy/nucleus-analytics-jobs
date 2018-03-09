package org.gooru.cassandra.event.processor.handler;

import org.gooru.cassandra.event.processor.kafka.MessageDispatcher;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public class KafkaHandler implements EventHandler {

    @Override
    public void handle(JsonObject eventData) {
        MessageDispatcher.getInstance().dispatch(eventData);
    }

}
