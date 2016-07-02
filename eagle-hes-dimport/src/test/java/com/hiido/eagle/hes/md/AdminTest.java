package com.hiido.eagle.hes.md;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

import com.hiido.eagle.hes.md.EsMdFactory;

public class AdminTest {
	
	private TransportClient client = null;
	
	public AdminTest() {
		Settings.Builder settings = Settings.builder()
				.put("discovery.zen.ping.multicast.enabled", "false")
                .put("index.gateway.type", "none")
                .put("gateway.type", "none")
                .put("transport.tcp.compress", "true")
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s")
				.put("cluster.name", "sealdsp");
		
		client = TransportClient.builder().settings(settings).build();
		client.addTransportAddresses(
				new InetSocketTransportAddress(new InetSocketAddress("kewn", 9300)));
	}
	
	@Test
	public void test01() throws InterruptedException, ExecutionException {
		String indexName = "idx_test_transfer18";
		
		CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexName);
		CloseIndexResponse closeResp = client.admin().indices().close(closeIndexRequest).get();         
		System.out.println(closeResp);
		
		OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
		OpenIndexResponse openResp = EsMdFactory.getTransportClient().admin().indices().open(openIndexRequest).get();
		System.out.println(openResp);
	}
	
	@Test
	public void test02() throws InterruptedException, ExecutionException {
		String indexName = "idx_test_transfer_1";
		
		IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
		IndicesExistsResponse existsResp = EsMdFactory.getTransportClient().admin().indices().exists(existsRequest).get();
		if (existsResp.isExists()) {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
			DeleteIndexResponse deleteResp = EsMdFactory.getTransportClient().admin().indices().delete(deleteIndexRequest).get();         
			System.out.println(deleteResp);
		}
		
		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse createResp = EsMdFactory.getTransportClient().admin().indices().create(createIndexRequest).get();
		System.out.println(createResp);
	}

}
