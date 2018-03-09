/**
 * 
 */
package org.gooru.jobs.sbl.event.handler.dispatcher;

import io.vertx.core.json.JsonObject;

/**
 * @author gooru
 *
 */
public interface Dispatcher {

	public void dispatch(JsonObject eventBody);
}
