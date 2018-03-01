package org.gooru.jobs.sbl.event.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.gooru.jobs.sbl.event.handler.dispatcher.KafkaMessageDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 21-Feb-2018
 */
public class FileProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);
	private static final Logger ERROR_LOGGER = LoggerFactory.getLogger("org.gooru.event.error.logs");

	private static List<String> SUPPORTED_EVENTS = Arrays.asList("event.user.signin", "event.user.signout",
			"event.internal.lti.sso", "event.internal.wsfed.sso");

	public static void process(Path path) {
		try (Stream<String> streams = Files.lines(path)) {
			streams.forEach(line -> {
				JsonObject eventBody = processLine(line);
				if (eventBody != null) {
					LOGGER.debug("Dispatching event:{}", eventBody);
					KafkaMessageDispatcher.getInstance().dispatch(eventBody);
				} else {
					ERROR_LOGGER.error(line);
				}
			});
		} catch (IOException e) {
			LOGGER.error("error while processing log files", e);
		}
	}

	private static JsonObject processLine(String line) {
		JsonObject eventData = new JsonObject(line);
		JsonObject event = eventData.getJsonObject("event.dump");

		if (event == null || event.isEmpty()) {
			LOGGER.debug("event details not found, aborting");
			return null;
		}

		String eventName = event.getString("event.name");
		if (eventName == null || !SUPPORTED_EVENTS.contains(eventName)) {
			LOGGER.debug("unsupported event type found, skipping");
			return null;
		}

		return new EventBodyBuilder(event).build();
	}
}
