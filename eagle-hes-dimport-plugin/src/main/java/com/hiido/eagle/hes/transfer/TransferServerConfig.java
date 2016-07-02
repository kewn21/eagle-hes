package com.hiido.eagle.hes.transfer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

@SuppressWarnings("unchecked")
public class TransferServerConfig {
	
	private static final Logger LOG = LoggerFactory.getLogger(TransferServerConfig.class);

	public static String SERVER_HOST;
	public static int SERVER_PORT;
	public static int SERVER_WORK_NUM;
	public static ArrayList<String> ES_ROOT_DATA_PATHS;
	public static String ES_CLUSTER_NAME;
	
	static {
		Properties pros = null;
		InputStream input = null;
		try {
			input = TransferServerConfig.class.getClassLoader().getResourceAsStream("conf/hes-server.properties");
			pros = new Properties();
			pros.load(input);
		} catch (Exception e) {
			LOG.error("load properties err", e);
		} finally {
			Closeables.closeQuietly(input);
		}
		
		if (pros != null) {
			try {
				InetAddress addr = InetAddress.getLocalHost();
				SERVER_HOST = addr.getHostName(); 
			} catch (Exception e) {
				LOG.error("Set SERVER_HOST err", e);
			}
			
			SERVER_PORT = Integer.valueOf(pros.getProperty("SERVER_PORT"));
			SERVER_WORK_NUM = Integer.valueOf(pros.getProperty("SERVER_WORK_NUM"));
			
			Map<String, Object> esConfig = null;
			input = null;
			try {
				String esRootPath = pros.getProperty("ES_ROOT_PATH");
				input = new FileInputStream(esRootPath + "/config/elasticsearch.yml");
				Yaml yaml = new Yaml();
				esConfig = (Map<String, Object>) yaml.load(input);
			} catch (Exception e) {
				LOG.error("Load es config err", e);
			} finally {
				Closeables.closeQuietly(input);
			}
			
			if (esConfig != null) {
				if (esConfig.containsKey("path.data")) {
					ES_ROOT_DATA_PATHS = Lists.newArrayList(Splitter.on(",").split(esConfig.get("path.data").toString()));
				}
				if (esConfig.containsKey("cluster.name")) {
					ES_CLUSTER_NAME = esConfig.get("cluster.name").toString();
				}
			}
		}
		
	}

}
