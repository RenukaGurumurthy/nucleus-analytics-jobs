package org.gooru.cassandra.event.processor.components;

import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import io.vertx.core.json.JsonObject;

/**
 * @author szgooru Created On: 27-Feb-2018
 */
public class DataSourceRegistry {
    
    private BasicDataSource connectionPool;
    private volatile boolean initialized = false;

    private DataSourceRegistry() {
    }

    public void init(JsonObject config) {
        if (!initialized) {
            connectionPool = initDataSource(config);
            initialized = true;
        }
    }

    public void finalize() {
        try {
            connectionPool.close();
        } catch (SQLException e) {
            e.printStackTrace();
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

    public BasicDataSource getDataSource() {
        return connectionPool;
    }

    private static final class Holder {
        private static final DataSourceRegistry INSTANCE = new DataSourceRegistry();
    }
}
