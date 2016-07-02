package com.hiido.eagle.hes.benchmark;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;
import com.hiido.eagle.hes.transfer.ClientBootstrapFactory;
import com.hiido.eagle.hes.transfer.FileTransferBootstrapFactory;
import com.hiido.eagle.hes.transfer.FileTransferClient;
import com.hiido.eagle.hes.transfer.TransferClientConfig;

public class MutipleFileTransferTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(MutipleFileTransferTest.class);
	
	private final static String host = "127.0.0.1";
	
	@Test
	public void syncFolder() {
		
		long startTime = System.currentTimeMillis();
		LOG.info("Start to syc folder");
		
		File folder = new File("/opt/work/data/datafile");
		
		ExecutorService executorService = Executors.newFixedThreadPool(folder.listFiles().length);
		
		final CountDownLatch latch = new CountDownLatch(folder.listFiles().length);
		
		final ClientBootstrapFactory clientBootstrapFactory = new FileTransferBootstrapFactory();
		
		for (final File file : folder.listFiles()) {
			if (file.isDirectory()) {
				latch.countDown();
				continue;
			}
			/*if (!file.getName().equals("test.txt")) {
				latch.countDown();
				continue;
			}*/
			
			executorService.execute(new Runnable() {
				
				@Override
				public void run() {
					
					FileTransferClient client = null;
					try {
						client = new FileTransferClient(host, TransferClientConfig.SERVER_PORT, clientBootstrapFactory);
						client.transfer(file.getPath());
					} catch (IOException | InterruptedException e) {
						LOG.error("sync {} err", file.getPath(), e);
					} finally {
						try {
							Closeables.close(client, true);
						} catch (Exception e2) {
							LOG.error("close {} err", file.getPath(), e2);
						} finally {
							latch.countDown();
						}
					}
					
				}
			});
			
		}
		
		try {
			latch.await();
		} catch (InterruptedException e) {
		} finally {
			clientBootstrapFactory.shutdown();
		}
		
		long endTime = System.currentTimeMillis();
		LOG.error("Finish to sync folder, cost time {}", endTime - startTime);
		
		executorService.shutdownNow();
	}

}
