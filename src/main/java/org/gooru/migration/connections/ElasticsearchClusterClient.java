package org.gooru.migration.connections;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchClusterClient {

	private static Client client = null;
	private static final ConfigSettingsLoader configSettingsLoader = ConfigSettingsLoader.instance();
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClusterClient.class);

	ElasticsearchClusterClient() {
		try {
			initializeElsConnection(configSettingsLoader.getSearchElsCluster(),
					configSettingsLoader.getSearchElsHost());
			LOG.info("ELS Cluster initialized successfully..");
		} catch (Exception e) {
			LOG.error("Error while initializing els cluster...");
		}
	}

	private static class ElasticsearchClusterClientHolder {
		public static final ElasticsearchClusterClient INSTANCE = new ElasticsearchClusterClient();
	}

	public static ElasticsearchClusterClient instance() {
		return ElasticsearchClusterClientHolder.INSTANCE;
	}

	private static void initializeElsConnection(String cluster, String hosts) throws UnknownHostException {
		Settings settings = Settings.settingsBuilder().put("cluster.name", cluster).put("client.transport.sniff", true)
				.build();
		TransportClient transportClient = TransportClient.builder().settings(settings).build()
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hosts), 9300));
		client = transportClient;
	}

	public Client getElsClient() {
		return client;
	}
}
