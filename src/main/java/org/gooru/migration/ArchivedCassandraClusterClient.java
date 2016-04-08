package org.gooru.migration;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public final class ArchivedCassandraClusterClient {

	private static Keyspace cassandraKeyspace = null;

	ArchivedCassandraClusterClient(String host, String datacenter, String clusterName, String keyspaceName) {
		initializeCluster(host, datacenter, clusterName, keyspaceName);
	}

	public ArchivedCassandraClusterClient() {
		this(null, null, null, null);
	}

	public static void initializeCluster(String host, String datacenter, String clusterName, String keyspaceName) {
		ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
				.setPort(9160).setSeeds(host).setSocketTimeout(30000).setMaxTimeoutWhenExhausted(2000)
				.setMaxConnsPerHost(10).setInitConnsPerHost(1);
		poolConfig.setLocalDatacenter(datacenter);
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(clusterName)
				.forKeyspace(keyspaceName)
				.withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setTargetCassandraVersion("2.1.4")
						.setCqlVersion("3.0.0").setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
						.setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
				.withConnectionPoolConfiguration(poolConfig)
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		context.start();
		cassandraKeyspace = (Keyspace) context.getClient();

	}

	public Keyspace getCassandraKeyspace() {
		return cassandraKeyspace;
	}

	private static class ArchivedCassandraClusterClientHolder {
		public static final ArchivedCassandraClusterClient INSTANCE = new ArchivedCassandraClusterClient();
	}

	public static ArchivedCassandraClusterClient instance() {
		return ArchivedCassandraClusterClientHolder.INSTANCE;
	}
}
