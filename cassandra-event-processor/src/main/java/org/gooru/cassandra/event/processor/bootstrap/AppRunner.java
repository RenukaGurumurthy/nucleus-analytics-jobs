package org.gooru.cassandra.event.processor.bootstrap;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Scanner;

import org.gooru.cassandra.event.processor.CassandraEventProcessor;
import org.gooru.cassandra.event.processor.components.DataSourceRegistry;
import org.gooru.cassandra.event.processor.components.KafkaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

public class AppRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AppRunner.class);
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyyMMddkkmm");
    private static final String CASSANDRA_CONFIG = "cassandra";
    private static final String KAFKA_PRODUCER_SETTINGS = "kafka.producer.settings";
    private static final String REPORTS_DB = "reports_db";

    private static JsonObject config;
    private static Long startTime = null;
    private static long processingStartTime;
    
    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Invalid arguments passed");
        }

        processingStartTime = System.currentTimeMillis();
        AppRunner runner = new AppRunner();
        AppRunner.initializeConfig(args[0]);

        String strStartTime = args[1];
        if (strStartTime == null || strStartTime.isEmpty()) {
            throw new IllegalArgumentException(
                "missing start time argument. Format: yyyyMMddkkmm Example: 201801010000");
        }

        try {
            startTime = DATE_FORMATTER.parse(strStartTime).getTime();
        } catch (ParseException e) {
            LOGGER.debug("unable to parse the start time");
            throw new IllegalArgumentException(e);
        }

        LOGGER.info("event processing started with startTime:{}", strStartTime);

        runner.run();
        LOGGER.debug("Finished processing in {}ms", (System.currentTimeMillis() - processingStartTime));
        System.exit(0);
    }

    private void run() {
        setupSystemProperties();
        Session session = initializeCassandraSession();

        initializeDataSource();
        //initializeKafkaRegistry();

        LOGGER.debug("data source initialized.. now start processing");
        new CassandraEventProcessor(session).process(startTime);

        session.close();
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

    private Session initializeCassandraSession() {
        JsonObject cassandraConfig = config.getJsonObject(CASSANDRA_CONFIG);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 4, 4).setConnectionsPerHost(HostDistance.REMOTE, 2, 2)
            .setMaxRequestsPerConnection(HostDistance.LOCAL, 4096).setMaxRequestsPerConnection(HostDistance.REMOTE, 512)
            .setIdleTimeoutSeconds(30);

        Cluster cluster = Cluster.builder().withClusterName(cassandraConfig.getString("cluster"))
            .addContactPoints(cassandraConfig.getString("hosts").split(","))
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withPoolingOptions(poolingOptions)
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
        return cluster.connect(cassandraConfig.getString("keyspace"));
    }

    private static void initializeDataSource() {
        LOGGER.debug("initializing datasource..");
        DataSourceRegistry.getInstance().init(config.getJsonObject(REPORTS_DB));
    }

    private static void initializeKafkaRegistry() {
        JsonObject producerConfig = config.getJsonObject(KAFKA_PRODUCER_SETTINGS);
        KafkaRegistry.getInstance().initializeComponent(producerConfig);
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
