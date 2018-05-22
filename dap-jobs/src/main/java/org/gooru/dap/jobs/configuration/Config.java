package org.gooru.dap.jobs.configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class Config {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
    private static final Config ourInstance = new Config();
    private final JsonNode rootConfigNode;

    static Config getInstance() {
        return ourInstance;
    }

    private Config() {
        try {
            final String configFile = System.getProperty("config.file");
            LOGGER.info("The config file used is '{}", configFile);
            if (configFile != null) {
                byte[] configData = Files.readAllBytes(Paths.get(configFile));
                ObjectMapper objectMapper = new ObjectMapper();
                rootConfigNode = objectMapper.readTree(configData);
            } else {
                throw new IllegalArgumentException("Invalid config file");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Not able to initialize configuration", e);
        }
    }

    JsonNode getRootConfig() {
        return rootConfigNode;
    }
}
