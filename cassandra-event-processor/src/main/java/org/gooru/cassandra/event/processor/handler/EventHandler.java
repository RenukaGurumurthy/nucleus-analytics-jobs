package org.gooru.cassandra.event.processor.handler;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public interface EventHandler {
    
    public void handle(JsonObject eventData);
}
