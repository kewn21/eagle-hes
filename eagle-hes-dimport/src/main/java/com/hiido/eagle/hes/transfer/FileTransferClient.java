package com.hiido.eagle.hes.transfer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.AttributeKey;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;
import com.hiido.eagle.hes.dimport.common.Conts;

public class FileTransferClient implements Closeable {
	
	private static final Logger LOG = LoggerFactory.getLogger(FileTransferClient.class);
	
	protected static final AttributeKey<FileTransferFuture<Integer>> ATTR_FUTURE_KEY = AttributeKey.valueOf("future");
	
	protected final Channel channel;
	
	public FileTransferClient(String host, int port, ClientBootstrapFactory clientBootstrapFactory) throws InterruptedException {
		
		LOG.info("Start to connect host:{}, port:{}", host, port);

        ChannelFuture future = clientBootstrapFactory.getBootstrap().connect(host, port).sync(); 
        channel = future.channel();
        
        LOG.info("Connect to host:{}, port:{} successfully", host, port);
	}
	
	public <T> void transfer(final T file) throws IOException, InterruptedException {
		
		LOG.info("Sending file [{}] to server", file);
		
		final long startTime = System.currentTimeMillis();
		
		FileTransferFuture<Integer> future = new FileTransferFuture<Integer>();
		channel.attr(FileTransferClient.ATTR_FUTURE_KEY).set(future);
		channel.write(file);
		
		future.get();
		
		if (future.isSuccessed()) {
    		long endTime = System.currentTimeMillis();
        	LOG.info("Finished to sent file [{}] to server cost time : {} ms", file, endTime - startTime);
    	}
    	else {
    		throw new IOException("Sent file [" + file + "] to server err");
		}
	}
	
	@Override
	public void close() throws IOException {
		try {
			channel.closeFuture().sync();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
    
    public static class WriteStreamHandler extends ChannelOutboundHandlerAdapter {  
    	
    	@Override
    	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    		transferFile(ctx, msg, promise);
    	}
    	
    	protected void transferFile(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        	File file = (File)msg;
    		String fileName = file.getName();
    		RandomAccessFile accessfile = new RandomAccessFile(file, "r");
    		
    		ByteBuf buf = ctx.alloc().buffer(32 + fileName.length() + 64);
    		buf.writeInt(fileName.length());
    		buf.writeBytes(fileName.getBytes());  
    		buf.writeLong(accessfile.length());
            ctx.write(buf);  
            
            ChunkedFile chunkedFile = new ChunkedFile(accessfile);
    		ctx.writeAndFlush(chunkedFile, promise);
    	}
    }
    
    public static class ServerAskHandler extends ChannelInboundHandlerAdapter { 
    	
    	@Override
    	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    		ByteBuf in = (ByteBuf) msg;
    		byte ret = in.readByte();
    		FileTransferFuture<Integer> future = ctx.channel().attr(FileTransferClient.ATTR_FUTURE_KEY).get();
    		if (ret == Conts.TRANSFER_SUC) {
    			LOG.info("Server asked [ret {}] indicate transfer successfully,so the channel should be closed", Conts.TRANSFER_SUC);
    			ctx.channel().close();
    			future.onSuccess(1);
			}
    		else {
    			LOG.error("Server asked [ret {}] indicate transfer fail,so that it should to be re-transfered", Conts.TRANSFER_ERR);
    			ctx.channel().close();
    			future.onFailure(new IOException("Transfer file err"));
    		}
    	}
    }
    
    
	public static void main(String[] args) throws Exception {
		
		//String file = "/opt/work/data/datafile/test1.txt";
		String file = "E://test.txt";
		
		ClientBootstrapFactory clientBootstrapFactory = new ClientBootstrapFactory();
		clientBootstrapFactory.getBootstrap().handler(new ChannelInitializer<SocketChannel>() {  
            @Override  
            public void initChannel(SocketChannel ch) throws Exception {  
           	 ch.pipeline().addLast(new ChunkedWriteHandler());
           	 ch.pipeline().addLast(new WriteStreamHandler());
           	 
           	 ch.pipeline().addLast(new ServerAskHandler());
            }  
        }); 
		
		FileTransferClient client = null;
		try {
			client = new FileTransferClient("127.0.0.1", 8081, clientBootstrapFactory);
			client.transfer(file);
		} catch (IOException | InterruptedException e) {
			System.out.println("transfer err");
			throw e;
		} finally {
			try {
				Closeables.close(client, true);
			} catch (Exception e2) {
				System.out.println("close client err");
			}
			clientBootstrapFactory.shutdown();
		}
		
		System.out.println("Sync file finished");
	}

}
