package org.gooru.jobs.sbl.event.handler.bootstrap;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

import org.gooru.jobs.sbl.event.handler.components.DataSourceRegistry;
import org.gooru.jobs.sbl.event.handler.components.KafkaRegistry;
import org.gooru.jobs.sbl.event.processor.FileProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 21-Feb-2018
 */
public class SBLLoginEventHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(SBLLoginEventHandler.class);
	private static JsonObject config;

	public static void main(String[] args) {
		if (args.length != 1) {
			throw new IllegalArgumentException("missing configuration file argument");
		}

		SBLLoginEventHandler eventHandler = new SBLLoginEventHandler();
		SBLLoginEventHandler.initializeConfig(args[0]);
		DataSourceRegistry.getInstance().init(config.getJsonObject("db.settings"));
		KafkaRegistry.getInstance().initializeComponent(config.getJsonObject("kafka.producer.settings"));
		eventHandler.handle();
	}

	private void handle() {
		// Initialize logger
		setupSystemProperties();

		LOGGER.info("starting handling log files");
		String logDirectory = config.getString("log.directory");
		try (Stream<Path> paths = Files.walk(Paths.get(logDirectory))) {
			paths.filter(Files::isRegularFile).forEach(path -> {
				LOGGER.info("processing file: {}", path.getFileName());
				FileProcessor.process(path);
			});
		} catch (Throwable t) {
			LOGGER.error("error while processing log files", t);
		}

		DataSourceRegistry.getInstance().finalize();
	}

	private static void setupSystemProperties() {
		JsonObject systemProperties = config.getJsonObject("systemProperties");
		for (Map.Entry<String, Object> property : systemProperties) {
			String propValue = systemProperties.getString(property.getKey());
			System.setProperty(property.getKey(), propValue);
		}
		String logbackFile = System.getProperty("logback.configurationFile");
		if (logbackFile != null && !logbackFile.isEmpty()) {
			setupLoggerMachinery(logbackFile);
		}
	}

	private static void setupLoggerMachinery(String logbackFile) {
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

		try {
			JoranConfigurator configurator = new JoranConfigurator();
			configurator.setContext(context);
			context.reset();
			configurator.doConfigure(logbackFile);
		} catch (JoranException je) {
			// StatusPrinter will handle this
		}

		StatusPrinter.printInCaseOfErrorsOrWarnings(context);
	}

	private static void initializeConfig(String filePath) {
		if (filePath != null) {
			try (Scanner scanner = new Scanner(new File(filePath)).useDelimiter("\\A")) {
				String sconf = scanner.next();
				try {
					config = new JsonObject(sconf);
				} catch (DecodeException e) {
					LOGGER.error("Configuration file " + sconf + " does not contain a valid JSON object");
					throw e;
				}
			} catch (FileNotFoundException e) {
				try {
					config = new JsonObject(filePath);
				} catch (DecodeException de) {
					LOGGER.error("Argument does not point to a file and is not valid JSON: " + filePath);
					throw de;
				}
			}
		} else {
			LOGGER.error("Null file path");
			throw new IllegalArgumentException("Null configuration file");
		}
	}
}
