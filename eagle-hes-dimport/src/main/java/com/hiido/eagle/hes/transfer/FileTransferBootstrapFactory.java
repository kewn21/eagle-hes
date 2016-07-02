package com.hiido.eagle.hes.transfer;

import com.hiido.eagle.hes.transfer.FileTransferClient.ServerAskHandler;
import com.hiido.eagle.hes.transfer.FileTransferClient.WriteStreamHandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileTransferBootstrapFactory extends ClientBootstrapFactory {
	
	public FileTransferBootstrapFactory() {
		super();
		
		bootstrap.handler(new ChannelInitializer<SocketChannel>() {  
	        @Override  
	        public void initChannel(SocketChannel ch) throws Exception {  
	       	 ch.pipeline().addLast(new ChunkedWriteHandler());
	       	 ch.pipeline().addLast(new WriteStreamHandler());
	       	 
	       	 ch.pipeline().addLast(new ServerAskHandler());
	        }  
		});  
	}

}
