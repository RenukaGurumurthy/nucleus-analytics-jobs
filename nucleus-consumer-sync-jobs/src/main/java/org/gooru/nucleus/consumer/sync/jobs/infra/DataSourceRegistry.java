package org.gooru.nucleus.consumer.sync.jobs.infra;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public final class DataSourceRegistry {
  private static final String CORE_DATA_SOURCE = "coreDataSource";
  private static final String ANALYTICS_DATA_SOURCE = "analyticsDataSource";
  private static final String DEFAULT_DATA_SOURCE_TYPE = "nucleus.ds.type";
  private static final String DS_HIKARI = "hikari";
  private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceRegistry.class);
  // All the elements in this array are supposed to be present in config file
  // as keys as we are going to initialize them with the value associated with
  // that key
  private final List<String> datasources = Arrays.asList(CORE_DATA_SOURCE, ANALYTICS_DATA_SOURCE);
  private final Map<String, DataSource> registry = new HashMap<>();
  private volatile boolean initialized;

  private DataSourceRegistry() {
    // TODO Auto-generated constructor stub
  }

  public static DataSourceRegistry getInstance() {
    return Holder.INSTANCE;
  }

  public void initializeComponent(JSONObject config) {
    // Skip if we are already initialized
    LOGGER.debug("Initialization called upon.");
    if (!initialized) {
      LOGGER.debug("May have to do initialization");
      // We need to do initialization, however, we are running it via verticle
      // instance which is going to run in
      // multiple threads hence we need to be safe for this operation
      synchronized (Holder.INSTANCE) {
        LOGGER.debug("Will initialize after double checking");
        if (!initialized) {
          LOGGER.debug("Initializing now");
          for (String datasource : datasources) {
            JSONObject dbConfig = config.getJSONObject(datasource);
            if (dbConfig != null) {
              DataSource ds = initializeDataSource(dbConfig);
              registry.put(datasource, ds);
            }
          }
          initialized = true;
        }
      }
    }
  }

  public DataSource getCoreDataSource() {
    return registry.get(CORE_DATA_SOURCE);
  }

  public DataSource getAnalyticsDataSource() {
    return registry.get(ANALYTICS_DATA_SOURCE);
  }

  public DataSource getDataSourceByName(String name) {
    if (name != null) {
      return registry.get(name);
    }
    return null;
  }

  private DataSource initializeDataSource(JSONObject dbConfig) {
    // The default DS provider is hikari, so if set explicitly or not set use
    // it, else error out
    String dsType = dbConfig.getString(DEFAULT_DATA_SOURCE_TYPE);
    if (dsType != null && !dsType.equals(DS_HIKARI)) {
      // No support
      throw new IllegalStateException("Unsupported data store type");
    }
    final HikariConfig config = new HikariConfig();

    dbConfig.keySet().forEach(key -> {
      switch (key) {
      case "dataSourceClassName":
        config.setDataSourceClassName(dbConfig.getString(key));
        break;
      case "jdbcUrl":
        config.setJdbcUrl(dbConfig.getString(key));
        break;
      case "username":
        config.setUsername(dbConfig.getString(key));
        break;
      case "password":
        config.setPassword(dbConfig.getString(key));
        break;
      case "autoCommit":
        config.setAutoCommit(dbConfig.getBoolean(key));
        break;
      case "connectionTimeout":
        config.setConnectionTimeout(dbConfig.getLong(key));
        break;
      case "idleTimeout":
        config.setIdleTimeout(dbConfig.getLong(key));
        break;
      case "maxLifetime":
        config.setMaxLifetime(dbConfig.getLong(key));
        break;
      case "connectionTestQuery":
        config.setConnectionTestQuery(dbConfig.getString(key));
        break;
      case "minimumIdle":
        config.setMinimumIdle(dbConfig.getInt(key));
        break;
      case "maximumPoolSize":
        config.setMaximumPoolSize(dbConfig.getInt(key));
        break;
      case "metricRegistry":
        throw new UnsupportedOperationException(key);
      case "healthCheckRegistry":
        throw new UnsupportedOperationException(key);
      case "poolName":
        config.setPoolName(dbConfig.getString(key));
        break;
      case "initializationFailFast":
        config.setInitializationFailFast(dbConfig.getBoolean(key));
        break;
      case "isolationInternalQueries":
        config.setIsolateInternalQueries(dbConfig.getBoolean(key));
        break;
      case "allowPoolSuspension":
        config.setAllowPoolSuspension(dbConfig.getBoolean(key));
        break;
      case "readOnly":
        config.setReadOnly(dbConfig.getBoolean(key));
        break;
      case "registerMBeans":
        config.setRegisterMbeans(dbConfig.getBoolean(key));
        break;
      case "catalog":
        config.setCatalog(dbConfig.getString(key));
        break;
      case "connectionInitSql":
        config.setConnectionInitSql(dbConfig.getString(key));
        break;
      case "driverClassName":
        config.setDriverClassName(dbConfig.getString(key));
        break;
      case "transactionIsolation":
        config.setTransactionIsolation(dbConfig.getString(key));
        break;
      case "validationTimeout":
        config.setValidationTimeout(dbConfig.getLong(key));
        break;
      case "leakDetectionThreshold":
        config.setLeakDetectionThreshold(dbConfig.getLong(key));
        break;
      case "dataSource":
        throw new UnsupportedOperationException(key);
      case "threadFactory":
        throw new UnsupportedOperationException(key);
      case "datasource":
        JSONObject datasource = dbConfig.getJSONObject(key);
        datasource.keySet().forEach(dataKey -> {
          config.addDataSourceProperty(dataKey, datasource.getString(dataKey));
        });

        break;
      }
    });

    return new HikariDataSource(config);

  }

  private static final class Holder {
    private static final DataSourceRegistry INSTANCE = new DataSourceRegistry();
  }

}
