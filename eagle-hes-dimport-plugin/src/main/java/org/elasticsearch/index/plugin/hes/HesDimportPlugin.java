package org.elasticsearch.index.plugin.hes;

import java.util.Collection;
import java.util.Collections;

import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.plugin.hes.module.HesDimportModule;
import org.elasticsearch.plugins.Plugin;

public class HesDimportPlugin extends Plugin {
	
	ESLogger logger = Loggers.getLogger(HesDimportPlugin.class);

	@Override
	public String description() {
		logger.info("get the desc of hes-dimport");
		return "hes-dimport";
	}

	@Override
	public String name() {
		logger.info("get the name of hes-dimport");
		return "hes-dimport";
	}
	
    @Override
    public Collection<Module> nodeModules() {
    	logger.info("creating new nodeModules of hes-dimport");
        return Collections.<Module>singletonList(new HesDimportModule());
    }

}
