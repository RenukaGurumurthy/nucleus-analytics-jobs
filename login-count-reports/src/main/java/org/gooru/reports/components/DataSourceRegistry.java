package org.gooru.reports.components;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.gooru.reports.constants.AppConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public class DataSourceRegistry {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceRegistry.class);
	
	private final List<String> datasources = Arrays.asList(AppConstants.CORE_DATA_SOURCE, AppConstants.REPORTS_DATA_SOURCE);
    
    private volatile boolean initialized = false;
    private final Map<String, DataSource> registry = new HashMap<>();
    
    private DataSourceRegistry() {
    }

    public void init(JsonObject config) {
        if (!initialized) {
            for (String datasource : datasources) {
                JsonObject dbConfig = config.getJsonObject(datasource);
                if (dbConfig != null) {
                    try {
                        DataSource ds = initDataSource(dbConfig);
                        LOGGER.info("initialized datasource:{}", datasource);
                        registry.put(datasource, ds);
                    } catch (Throwable e) {
                        LOGGER.debug("Failed to create data source", e);
                        throw new IllegalStateException("Failed to create data source");
                    }
                }
            }
            
            initialized = true;
        }
    }

    public void finalize() {
    	for (String datasource : datasources) {
            DataSource ds = registry.get(datasource);
            if (ds != null) {
                if (ds instanceof BasicDataSource) {
                    try {
                    	LOGGER.info("closing datasource:{}", datasource);
						((BasicDataSource) ds).close();
					} catch (SQLException e) {
						LOGGER.warn("error while closing the connection", e);
					}
                }	
            }
        }
    }

    private BasicDataSource initDataSource(JsonObject config) {
        BasicDataSource ds = new BasicDataSource();
        ds.setUsername(config.getString("db.username"));
        ds.setPassword(config.getString("db.password"));
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUrl(config.getString("db.url"));
        ds.setInitialSize(config.getInteger("db.pool.size"));
        ds.setDefaultAutoCommit(config.getBoolean("db.autocommit"));
        return ds;
    }

    public static DataSourceRegistry getInstance() {
        return Holder.INSTANCE;
    }

    public BasicDataSource getDataSource(String name) {
        return (BasicDataSource) registry.get(name);
    }

    private static final class Holder {
        private static final DataSourceRegistry INSTANCE = new DataSourceRegistry();
    }
}
