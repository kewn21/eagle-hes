package com.hiido.eagle.hes.md;

import java.net.InetSocketAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.hiido.eagle.hes.transfer.TransferClientConfig;

public class EsMdFactory {
	
	private static TransportClient client = null;
	
	static {
		if (client == null) {
			Settings.Builder settings = Settings.builder()
					.put("discovery.zen.ping.multicast.enabled", "false")
	                .put("index.gateway.type", "none")
	                .put("gateway.type", "none")
	                //.put("transport.tcp.compress", "true")
	                .put("client.transport.ping_timeout", "30s")
	                .put("client.transport.nodes_sampler_interval", "30s")
					.put("cluster.name", TransferClientConfig.ES_CLUSTER_NAME);
			
			client = TransportClient.builder().settings(settings).build();
			
			for (String host : TransferClientConfig.ES_NODE_HOSTS) {
				client.addTransportAddresses(
						new InetSocketTransportAddress(new InetSocketAddress(host, 9300)));
			}
		}
	}
	
	public static TransportClient getTransportClient() {
		return client;
	}
	
	public static TransportClient createTransportClient() {
		Settings.Builder settings = Settings.builder()
				.put("discovery.zen.ping.multicast.enabled", "false")
                .put("index.gateway.type", "none")
                .put("gateway.type", "none")
                //.put("transport.tcp.compress", "true")
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s")
				.put("cluster.name", TransferClientConfig.ES_CLUSTER_NAME);
		
		TransportClient client = TransportClient.builder().settings(settings).build();
		
		for (String host : TransferClientConfig.ES_NODE_HOSTS) {
			client.addTransportAddresses(
					new InetSocketTransportAddress(new InetSocketAddress(host, 9300)));
		}
		
		return client;
	}
	

}
