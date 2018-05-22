package org.gooru.dap.jobs.configuration;

import com.fasterxml.jackson.databind.JsonNode;

public final class JobConfiguration {

    private JobConfiguration() {
        throw new AssertionError();
    }

    public static JsonNode fetchSystemPropsConfig() {
        return Config.getInstance().getRootConfig().get("systemProperties");
    }

    public static JsonNode fetchDataSources() {
        return Config.getInstance().getRootConfig().get("datasources");
    }

}
