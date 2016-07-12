package org.gooru.analytics.jobs.infra;

import org.gooru.analytics.jobs.constants.Constants;
import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;

import io.vertx.core.json.JsonObject;

public final class ArchievedCassandraClusterDataStax implements Initializer, Finalizer {

  private static Session session = null;
  private static final Logger LOG = LoggerFactory.getLogger(ArchievedCassandraClusterDataStax.class);
  private static String archivedCassSeeds = null;
  private static String archivedCassDatacenter = null;
  private static String archivedCassCluster = null;
  private static String archivedCassKeyspace = null;

  public Session getCassandraSession() {
    return session;
  }

  public String getArchivedCassKeyspace() {
    return archivedCassKeyspace;
  }

  private static class ArchievedCassandraClusterDataStaxClientHolder {
    public static final ArchievedCassandraClusterDataStax INSTANCE = new ArchievedCassandraClusterDataStax();
  }

  public static ArchievedCassandraClusterDataStax instance() {
    return ArchievedCassandraClusterDataStaxClientHolder.INSTANCE;
  }

  public void initializeComponent(JsonObject config) {
    archivedCassSeeds = config.getString("archived.cassandra.seeds");
    archivedCassDatacenter = config.getString("archived.cassandra.datacenter");
    archivedCassCluster = config.getString("archived.cassandra.cluster");
    archivedCassKeyspace = config.getString("archived.cassandra.keyspace");
    
    LOG.info("archivedCassSeeds : {} - archivedCassKeyspace : {} ", archivedCassSeeds, archivedCassKeyspace);

    Cluster cluster = Cluster.builder().withClusterName(archivedCassCluster).addContactPoints(archivedCassSeeds.split(Constants.COMMA))
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE).withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000)).build();
    session = cluster.connect(archivedCassKeyspace);

    LOG.info("Archived Cassandra initialized successfully using datastax driver...");
  }

  public void finalizeComponent() {
    session.close();
  }
}
