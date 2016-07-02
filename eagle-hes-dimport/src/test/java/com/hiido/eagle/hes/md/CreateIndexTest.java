package com.hiido.eagle.hes.md;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**  
 * @author dengqibin
 * @date 2016年3月29日 下午2:38:35
 */
public class CreateIndexTest {
	
	protected final Logger LOG = LoggerFactory.getLogger(CreateIndexTest.class);
	
	private BulkRequestBuilder bulkRequestBuilder = null;
	private TransportClient client = null;
	
	public CreateIndexTest() {
		Settings.Builder settings = Settings.builder()
				.put("discovery.zen.ping.multicast.enabled", "false")
                //.put("index.mapping._id.indexed", "true")
                .put("index.gateway.type", "none")
                .put("gateway.type", "none")
                .put("transport.tcp.compress", "true")
				//.put("client.transport.sniff", true)
                //.put("client.transport.ignore_cluster_name", true)
                .put("client.transport.ping_timeout", "30s")
                .put("client.transport.nodes_sampler_interval", "30s")
				.put("cluster.name", "sealdsp");
		
		client = TransportClient.builder().settings(settings).build();
		client.addTransportAddresses(
				new InetSocketTransportAddress(new InetSocketAddress("kewn", 9300)));
		
		bulkRequestBuilder = client.prepareBulk();
	}
	
	@Test
	public void test01() throws IOException {
		for (int i = 0; i < 1; i++) {
			XContentBuilder xbuilder = XContentFactory.jsonBuilder();
			xbuilder.startObject();
			/*xbuilder.field("col_3", "1");
			xbuilder.field("col_4", "2");*/
			xbuilder.field("col_1", 2L);
			xbuilder.endObject();
			
			bulkRequestBuilder.add(client.prepareIndex("idx_test_transfer1", "idx_type", String.valueOf(i + 1)).setSource(xbuilder));
			
			BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet(); 
			
			if (bulkResponse.hasFailures()) { 
				LOG.error("commit bulk request err {}", bulkResponse.buildFailureMessage());
			}
			bulkRequestBuilder.request().requests().clear();
		}
	}

}
