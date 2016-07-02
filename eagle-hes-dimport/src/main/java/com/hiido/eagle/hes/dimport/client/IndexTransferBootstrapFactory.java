package com.hiido.eagle.hes.dimport.client;

import com.hiido.eagle.hes.dimport.client.IndexTransferClient.IndexWriteStreamHandler;
import com.hiido.eagle.hes.transfer.ClientBootstrapFactory;
import com.hiido.eagle.hes.transfer.FileTransferClient.ServerAskHandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

public class IndexTransferBootstrapFactory extends ClientBootstrapFactory {
	
	public IndexTransferBootstrapFactory() {
		super();
		
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {  
	        @Override  
	        public void initChannel(SocketChannel ch) throws Exception {  
	       	 ch.pipeline().addLast(new ChunkedWriteHandler());
	       	 ch.pipeline().addLast(new IndexWriteStreamHandler());
	       	 
	       	 ch.pipeline().addLast(new ServerAskHandler());
	        }  
		});  
	}

}
