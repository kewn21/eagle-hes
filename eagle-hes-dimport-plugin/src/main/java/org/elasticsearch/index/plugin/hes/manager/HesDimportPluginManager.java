package org.elasticsearch.index.plugin.hes.manager;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.hiido.eagle.hes.dimport.server.IndexTransferServer;

public class HesDimportPluginManager {
	
	ESLogger logger = Loggers.getLogger(HesDimportPluginManager.class);
	
	@Inject
	public HesDimportPluginManager(final IndexTransferServer server) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					server.start();
				} catch (Exception e) {
					logger.error("start hes server err", e);
				}
			}
		}).start();
	}
}
