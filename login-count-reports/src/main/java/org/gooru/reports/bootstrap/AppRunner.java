package org.gooru.reports.bootstrap;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Scanner;

import org.gooru.reports.components.DataSourceRegistry;
import org.gooru.reports.constants.AppConstants;
import org.gooru.reports.generator.ReportsGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

public class AppRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
	private static JsonObject config;

	private static String frequency;
	private static String startDate;
	private static String endDate;

	public static void main(String[] args) {
		if (args.length != 4) {
			printArgumentInfo();
			throw new IllegalArgumentException("Invalid arguments passed");
		}

		AppRunner runner = new AppRunner();

		try {
			AppRunner.initializeConfig(args[0]);
			validateArguments(args);
			runner.run();

			// finalize
			DataSourceRegistry.getInstance().finalize();
		} catch (IllegalArgumentException ie) {
			printArgumentInfo();
			throw ie;
		} catch (Throwable t) {
			LOGGER.error("error while generating report", t);
		}

		System.exit(0);
	}

	private void run() {
		setupSystemProperties();
		initializeDataSource();

		new ReportsGenerator(frequency, startDate, endDate).generate();
	}

	private static void validateArguments(String[] args) {
		frequency = args[1];
		if (frequency != null && !AppConstants.VALID_FREQUENCIES.contains(frequency)) {
			throw new IllegalArgumentException("invalid frequency passed");
		}

		startDate = args[2];
		try {
			LocalDate.parse(startDate);
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(e);
		}

		endDate = args[3];
		try {
			LocalDate.parse(endDate);
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(e);
		}

		LOGGER.debug("Generating report for Frequency:{}, Start Date:{}, End Date:{}", frequency, startDate, endDate);
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

	private static void initializeDataSource() {
		LOGGER.debug("initializing datasource..");
		DataSourceRegistry.getInstance().init(config);
	}

	private static void printArgumentInfo() {
		System.out.println("=: Program Arguments :=");
		System.out.println("<config-file> <frequency> <start-date> <end-date>");
		System.out.println(
				"config-file:\t Config file path\nfrequency: \tY (Yearly), Q(Quartely), M (Monthly), W (Weekly), D (Daily)");
		System.out.println("start-date: \tstart date of the report. Format: YYYY-MM-DD");
		System.out.println("end-date: \tend date of the report. Format: YYYY-MM-DD\n\n");
	}
}
