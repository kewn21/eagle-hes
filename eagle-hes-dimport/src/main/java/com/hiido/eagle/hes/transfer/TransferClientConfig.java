package com.hiido.eagle.hes.transfer;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class TransferClientConfig {
	
	private static final Logger LOG = LoggerFactory.getLogger(TransferClientConfig.class);
	
	public static int SERVER_PORT;
	public static ArrayList<String> ES_NODE_HOSTS;
	public static String ES_CLUSTER_NAME;
	public static int CONCURRENT_NUM;
	
	static {
		Properties pros = null;
		InputStream input = null;
		try {
			input = TransferClientConfig.class.getClassLoader().getResourceAsStream("hes-client.properties");
			pros = new Properties();
			pros.load(input);
		} catch (Exception e) {
			LOG.error("load properties err", e);
		} finally {
			CloseableUtils.closeQuietly(input);
		}
		
		if (pros != null) {
			SERVER_PORT = Integer.valueOf(pros.getProperty("SERVER_PORT"));
			ES_NODE_HOSTS = Lists.newArrayList(Splitter.on(",").split(pros.getProperty("ES_NODE_HOSTS")));
			ES_CLUSTER_NAME = pros.getProperty("ES_CLUSTER_NAME");
			CONCURRENT_NUM = Integer.valueOf(pros.getProperty("CONCURRENT_NUM"));
		}
		
	}

}
