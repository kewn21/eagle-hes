package com.hiido.eagle.hes.md;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**  
 * @author dengqibin
 * @date 2016骞�鏈�9鏃�涓嬪崍2:38:35
 */
public class NodeCreateIndexTest {
	
	protected final Logger LOG = LoggerFactory.getLogger(NodeCreateIndexTest.class);
	
	private BulkRequestBuilder bulkRequestBuilder = null;
	private Node node = null;
	private Client client = null;
	
	public NodeCreateIndexTest() {
		Settings.Builder settings = Settings.builder()
				.put("http.enabled", false)
				.put("node.client", true)
				.put("path.home", "/opt/work/data/esclient")
				.put("cluster.name", "sealdsp");
		
		
		node =
			    nodeBuilder().clusterName("sealdsp").local(true)
			        .settings(settings)
			        .client(true)
			    .node();

		client = node.client();
		bulkRequestBuilder = client.prepareBulk();
	}
	
	@Test
	public void test01() throws IOException {
		
		XContentBuilder xbuilder = XContentFactory.jsonBuilder();
		xbuilder.startObject();
		xbuilder.field("col1", "1");
		xbuilder.field("col2", "2");
		xbuilder.endObject();
		
		bulkRequestBuilder.add(client.prepareIndex("test-idx", "test-type").setSource(xbuilder));
		
		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet(); 
		
		if (bulkResponse.hasFailures()) { 
			LOG.error("commit bulk request err {}", bulkResponse.buildFailureMessage());
		}
		bulkRequestBuilder.request().requests().clear();
	}

}
