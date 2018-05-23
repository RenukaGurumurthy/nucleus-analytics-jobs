package org.gooru.dap.jobs.components;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;

import javax.sql.DataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

final class DatasourceHelper {
    private DatasourceHelper() {
        throw new AssertionError();
    }

    private static final String DEFAULT_DATA_SOURCE_TYPE = "ds.type";
    private static final String DS_HIKARI = "hikari";

    static DataSource initializeDataSource(JsonNode dbConfig) {
        // The default DS provider is hikari, so if set explicitly or not set
        // use it, else error out
        String dsType = dbConfig.get(DEFAULT_DATA_SOURCE_TYPE).textValue();
        if (!Objects.equals(dsType, DS_HIKARI)) {
            // No support
            throw new IllegalStateException("Unsupported data store type");
        }
        HikariConfig config = new HikariConfig();

        for (Iterator<Entry<String, JsonNode>> it = dbConfig.fields(); it.hasNext(); ) {
            Entry<String, JsonNode> entry = it.next();
            switch (entry.getKey()) {
            case "dataSourceClassName":
                config.setDataSourceClassName(entry.getValue().textValue());
                break;
            case "jdbcUrl":
                config.setJdbcUrl(entry.getValue().textValue());
                break;
            case "username":
                config.setUsername(entry.getValue().textValue());
                break;
            case "password":
                config.setPassword(entry.getValue().textValue());
                break;
            case "autoCommit":
                config.setAutoCommit(entry.getValue().booleanValue());
                break;
            case "connectionTimeout":
                config.setConnectionTimeout(entry.getValue().longValue());
                break;
            case "idleTimeout":
                config.setIdleTimeout(entry.getValue().longValue());
                break;
            case "maxLifetime":
                config.setMaxLifetime(entry.getValue().longValue());
                break;
            case "connectionTestQuery":
                config.setConnectionTestQuery(entry.getValue().textValue());
                break;
            case "minimumIdle":
                config.setMinimumIdle(entry.getValue().intValue());
                break;
            case "maximumPoolSize":
                config.setMaximumPoolSize(entry.getValue().intValue());
                break;
            case "metricRegistry":
                throw new UnsupportedOperationException(entry.getKey());
            case "healthCheckRegistry":
                throw new UnsupportedOperationException(entry.getKey());
            case "poolName":
                config.setPoolName(entry.getValue().textValue());
                break;
            case "isolationInternalQueries":
                config.setIsolateInternalQueries(entry.getValue().booleanValue());
                break;
            case "allowPoolSuspension":
                config.setAllowPoolSuspension(entry.getValue().booleanValue());
                break;
            case "readOnly":
                config.setReadOnly(entry.getValue().booleanValue());
                break;
            case "registerMBeans":
                config.setRegisterMbeans(entry.getValue().booleanValue());
                break;
            case "catalog":
                config.setCatalog(entry.getValue().textValue());
                break;
            case "connectionInitSql":
                config.setConnectionInitSql(entry.getValue().textValue());
                break;
            case "driverClassName":
                config.setDriverClassName(entry.getValue().textValue());
                break;
            case "transactionIsolation":
                config.setTransactionIsolation(entry.getValue().textValue());
                break;
            case "validationTimeout":
                config.setValidationTimeout(entry.getValue().longValue());
                break;
            case "leakDetectionThreshold":
                config.setLeakDetectionThreshold(entry.getValue().longValue());
                break;
            case "dataSource":
                throw new UnsupportedOperationException(entry.getKey());
            case "threadFactory":
                throw new UnsupportedOperationException(entry.getKey());
            case "datasource":
                for (Iterator<Entry<String, JsonNode>> dsIterator = (entry.getValue().fields());
                     dsIterator.hasNext(); ) {
                    Entry<String, JsonNode> key = dsIterator.next();
                    config.addDataSourceProperty(key.getKey(), key.getValue());
                }
                break;
            }
        }

        return new HikariDataSource(config);

    }

}
