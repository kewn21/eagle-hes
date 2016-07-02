package com.hiido.eagle.hes.benchmark;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.eagle.hes.transfer.FileTransferServer;
import com.hiido.eagle.hes.transfer.TransferServerConfig;

public class FileTransferServerTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(FileTransferServerTest.class);
	
	@Test
	public void test() {
		try {
        	FileTransferServer server = new FileTransferServer(TransferServerConfig.SERVER_PORT, TransferServerConfig.SERVER_WORK_NUM);
			server.start();
		} catch (Exception e) {
			LOG.error("Server err", e);
		}
	}

}
