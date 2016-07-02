package org.elasticsearch.index.plugin.hes.module;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.index.plugin.hes.manager.HesDimportPluginManager;

import com.hiido.eagle.hes.dimport.server.IndexTransferServer;

public class HesDimportModule extends AbstractModule {
	
    @Override
    protected void configure() {
        bind(IndexTransferServer.class).asEagerSingleton();
        bind(HesDimportPluginManager.class).asEagerSingleton();
    }
}
