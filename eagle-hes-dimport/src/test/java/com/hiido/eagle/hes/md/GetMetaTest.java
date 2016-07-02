package com.hiido.eagle.hes.md;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.health.ClusterShardHealth;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

import com.google.common.io.Closeables;

public class GetMetaTest {
	
	private TransportClient client = null;
	
	public GetMetaTest() {
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
	public void test01() {
		InputStreamStreamInput in = null;
		
		try {
			in = new InputStreamStreamInput(sendGet());
			ClusterState state = ClusterState.Builder.readFrom(in, null);
			System.out.println(state.prettyPrint());
		} catch (Exception e) {
			System.err.println(e);
		} finally {
			Closeables.closeQuietly(in);
		}
	}
	
	@Test
	public void test02() {
		ClusterHealthResponse resp = client.admin().cluster().prepareHealth("test-idx")   
        .setWaitForGreenStatus()                    
        .get();
		
		System.out.println(resp);
	}
	
	@Test
	public void test03() {
	    Murmur3HashFunction hashFunction = new Murmur3HashFunction();
	    ClusterStateResponse resp = client.admin().cluster().prepareState().setIndices("test-idx").get();
        ClusterState clusterState = resp.getState();
	    String key = "testkey";
	    int hash = hashFunction.hash(key);
        int shardId = MathUtils.mod(hash, clusterState.getRoutingTable().index("test-idx").primaryShardsActive());
        System.out.println(shardId);
    }
	
	@Test
	public void test04() throws InterruptedException, ExecutionException {
		ClusterStateResponse resp = client.admin().cluster().prepareState().setIndices("test-idx").get();
		ClusterState state = resp.getState();
		System.out.println(state.prettyPrint());
		
		//state.getRoutingTable().index("test-idx").shard(1).getShardId().getId()
		String nodeId = state.getRoutingTable().index("test-idx").shard(1).primaryShard().currentNodeId();
		System.out.println(nodeId);
		String nodeHost = state.getNodes().get(nodeId).getName();
		System.out.println(nodeHost);
	}
	
	@Test
	public void test05() {
		ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get(); 
		String clusterName = healths.getClusterName();              
		int numberOfDataNodes = healths.getNumberOfDataNodes();     
		int numberOfNodes = healths.getNumberOfNodes();             

		for (ClusterIndexHealth health : healths) {                 
		    String index = health.getIndex();                       
		    int numberOfShards = health.getNumberOfShards();        
		    int numberOfReplicas = health.getNumberOfReplicas();    
		    ClusterHealthStatus status = health.getStatus();  
		    System.out.println(status);
		    
		    for (Map.Entry<Integer, ClusterShardHealth>  shard : health.getShards().entrySet()) {
		    	System.out.println(shard.getKey());
		    	System.out.println(shard.getValue());
			}
		}
	}
	
	@Test
	public void test06() throws InterruptedException, ExecutionException {
		ClusterStateResponse resp = client.admin().cluster().prepareState().setIndices("test-idx").get();
		ClusterState state = resp.getState();
		System.out.println(state.prettyPrint());
		
		//state.getRoutingTable().index("test-idx").shard(1).getShardId().getId()
		
		
		System.out.println(state.getRoutingTable().index("test-idx"));
		
		int shardNum = state.getRoutingTable().index("test-idx").shards().size();
		System.out.println(shardNum);
		String nodeId = state.getRoutingTable().index("test-idx").shard(1).primaryShard().currentNodeId();
		System.out.println(nodeId);
		String nodeHost = state.getNodes().get(nodeId).getName();
		System.out.println(nodeHost);

		ShardRouting replicaShard = state.getRoutingTable().index("test-idx").shard(1).replicaShards().get(0);
		int shardId = replicaShard.getId();
		System.out.println(shardId);
		
		/*replicaShard = state.getRoutingTable().index("test-idx").shard(2).replicaShards().get(0);
		shardId = replicaShard.getId();
		System.out.println(shardId);*/
		
		nodeId = replicaShard.currentNodeId();
		System.out.println(nodeId);
		nodeHost = state.getNodes().get(nodeId).getName();
		System.out.println(nodeHost);
	}
	
	// HTTP GET request
	private InputStream sendGet() throws Exception {

		String url = "http://kewn:9200/_cluster/state";
		
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		//add request header
		//con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		return con.getInputStream();
	}

}
