package org.gooru.nucleus.replay.jobs.infra;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

public class CassandraClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

  private Session session;

  private static JSONObject config;

  public CassandraClient(JSONObject config) {
    LOGGER.info("Par cons" + config);
    if (config != null) {
      CassandraClient.config = config;
      this.initializeComponent();
    } else {
      LOGGER.warn("Config can not be null");
    }
  }

  public CassandraClient() {
    if (CassandraClient.config == null) {
      LOGGER.warn("Config should not be null");
    }
  }

  public static CassandraClient getInstance() {
    return Holder.INSTANCE;
  }

  public void initializeComponent() {
    try {
      JSONObject cassandraConfig = CassandraClient.config.getJSONObject("cassandra");

      Cluster cluster = Cluster.builder().withClusterName(cassandraConfig.getString("cluster"))
              .addContactPoints(cassandraConfig.getString("hosts").split(",")).withRetryPolicy(DefaultRetryPolicy.INSTANCE)
              .withPoolingOptions(poolingOptions()).withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
      setSession(cluster.connect(cassandraConfig.getString("keyspace")));
    } catch (Exception e) {
      LOGGER.error("Error while initializing cassandra : {}", e);
      throw e;
    }
  }

  public void finalizeComponent() {
    if (getSession() != null && !getSession().isClosed()) {
      getSession().close();
    }
  }

  private static PoolingOptions poolingOptions() {
    PoolingOptions poolingOptions = new PoolingOptions();
    poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 4, 4).setConnectionsPerHost(HostDistance.REMOTE, 2, 2)
            .setMaxRequestsPerConnection(HostDistance.LOCAL, 4096).setMaxRequestsPerConnection(HostDistance.REMOTE, 512).setIdleTimeoutSeconds(30)
            .setPoolTimeoutMillis(15000);
    return poolingOptions;
  }

  public Session getSession() {
    return session;
  }

  public void setSession(Session session) {
    this.session = session;
  }

  private static class Holder {
    private static final CassandraClient INSTANCE = new CassandraClient(CassandraClient.config);
  }

}
