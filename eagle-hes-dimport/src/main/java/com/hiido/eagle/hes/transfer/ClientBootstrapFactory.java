package com.hiido.eagle.hes.transfer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ClientBootstrapFactory {

	protected final EventLoopGroup group; 
	protected final Bootstrap bootstrap;
	
	public ClientBootstrapFactory() {
		this(TransferClientConfig.CONCURRENT_NUM);
	}
	
	public ClientBootstrapFactory(int concurrentNum) {
		group = new NioEventLoopGroup(concurrentNum);
		bootstrap = new Bootstrap();  
        bootstrap.group(group)  
         .channel(NioSocketChannel.class)  
         .option(ChannelOption.TCP_NODELAY, true);
	}
	
	public EventLoopGroup getGroup() {
		return group;
	}

	public Bootstrap getBootstrap() {
		return bootstrap;
	}
	
	public void shutdown() {
		group.shutdownGracefully();
	}
	
}