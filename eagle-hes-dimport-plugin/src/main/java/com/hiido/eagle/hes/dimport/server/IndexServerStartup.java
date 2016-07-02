package com.hiido.eagle.hes.dimport.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexServerStartup {
	
	private static final Logger LOG = LoggerFactory.getLogger(IndexServerStartup.class);
	
	public static void main(String[] args) {
		try {
			IndexTransferServer server = new IndexTransferServer(null);
			server.start();
		} catch (Exception e) {
			LOG.error("Starting server err", e);
		}
	}

}
