package org.gooru.dap.jobs;

import java.util.Iterator;
import java.util.Map;

import org.gooru.dap.jobs.components.AppInitializer;
import org.gooru.dap.jobs.configuration.JobConfiguration;
import org.gooru.dap.jobs.processors.ProcessorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class AppRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);

    public static void main(String[] args) {
        new AppRunner().run();
    }

    private void run() {
        initializeApplication();
        executeJobProcessor();
    }

    private void initializeApplication() {
        setupSystemProperties();
        setupLoggerMachinery();
        AppInitializer.initializeApp();
    }

    private void setupSystemProperties() {
        JsonNode systemPropsConfig = JobConfiguration.fetchSystemPropsConfig();
        for (Iterator<Map.Entry<String, JsonNode>> it = systemPropsConfig.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> property = it.next();
            final String propValue = property.getValue().textValue();
            LOGGER.debug("Property: '{}' is set to value '{}'", property.getKey(), propValue);
            if (propValue != null) {
                System.setProperty(property.getKey(), propValue);
            } else {
                throw new IllegalStateException("Invalid system property key value in config");
            }
        }
    }

    private void setupLoggerMachinery() {
        String logbackFile = System.getProperty("logback.configurationFile");
        if (logbackFile != null && !logbackFile.isEmpty()) {
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(logbackFile);
            } catch (JoranException je) {
                StatusPrinter.printInCaseOfErrorsOrWarnings(context);
                throw new IllegalStateException("Error while initialising Logger machinery");
            }
        } else {
            LOGGER.warn("Not able to find logback config file");
            throw new IllegalArgumentException("Invalid logback config file");
        }

    }

    private void executeJobProcessor() {
        final String jobName = System.getProperty("job.name");
        if (jobName != null) {
            ProcessorBuilder.lookupBuilder(jobName).build();
        } else {
            LOGGER.warn("Job name should not null or empty, it should be pass java properties example: -Djob.name");
        }
    }

}
