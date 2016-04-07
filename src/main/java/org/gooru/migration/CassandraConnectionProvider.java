package org.gooru.migration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public final class CassandraConnectionProvider {
	private static Cluster analyticsCassandraCluster = null;
	private static Session analyticsCassandraSession = null;
	private static Keyspace cassandraKeyspace = null;

	private static String old_cass_hosts = "127.0.0.1";
	private static String old_cass_datacenter = "datacenter1";
	private static String old_cass_cluster = "cassandra-qa";
	private static String old_cass_keyspace = "ks_qa";

	private static String new_cass_hosts = "127.0.0.1";
	private static String new_cass_datacenter = "datacenter1";
	private static String new_cass_cluster = "cassandra-qa";
	private static String new_cass_keyspace = "ks_insights";
	
	public CassandraConnectionProvider(){
		initializeNewCluster();
		initializeOldCluster();
	}
	public static void main(String args[]) {
		initializeNewCluster();
		initializeOldCluster();
		System.out.println("Connection established....");
		System.out.println("analyticsCassandraSession : " + analyticsCassandraSession.getLoggedKeyspace());
		System.out.println("cassandraKeyspace : " + cassandraKeyspace.getKeyspaceName());
	}
	public Session getAnalyticsCassandraSession(){
		return analyticsCassandraSession;
	}
	public Keyspace getCassandraKeyspace(){
		return cassandraKeyspace;
	}
	public String getNewKeyspaceName(){
		return new_cass_keyspace;
	}
	private static class CassandraClientHolder {
		public static final CassandraConnectionProvider INSTANCE = new CassandraConnectionProvider();
	}

	public static CassandraConnectionProvider instance() {
		return CassandraClientHolder.INSTANCE;
	}
	public static void initializeOldCluster() {
		ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
				.setPort(9160).setSeeds(old_cass_hosts).setSocketTimeout(30000).setMaxTimeoutWhenExhausted(2000)
				.setMaxConnsPerHost(10).setInitConnsPerHost(1);
		poolConfig.setLocalDatacenter(old_cass_datacenter);
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(old_cass_cluster)
				.forKeyspace(old_cass_keyspace)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setTargetCassandraVersion("2.1.4")
						.setCqlVersion("3.0.0").setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
						.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
				.withConnectionPoolConfiguration(poolConfig)
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		cassandraKeyspace = (Keyspace) context.getClient();

	}

	public static void initializeNewCluster() {
		analyticsCassandraCluster = Cluster.builder().withClusterName(new_cass_cluster).addContactPoint(new_cass_hosts)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 30000))
				.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(new_cass_datacenter)))
				.build();
		analyticsCassandraSession = analyticsCassandraCluster.connect(new_cass_keyspace);
	}
	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(),
				StringSerializer.get());

		return aggregateColumnFamily;
	}

}
