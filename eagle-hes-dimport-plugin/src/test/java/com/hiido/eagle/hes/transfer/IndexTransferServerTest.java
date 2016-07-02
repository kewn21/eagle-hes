package com.hiido.eagle.hes.transfer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.eagle.hes.dimport.server.IndexTransferServer;

public class IndexTransferServerTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(IndexTransferServerTest.class);
	
	@Test
	public void test() {
		try {
			IndexTransferServer server = new IndexTransferServer(null);
			server.start();
		} catch (Exception e) {
			LOG.error("Server err", e);
		}
	}

}
