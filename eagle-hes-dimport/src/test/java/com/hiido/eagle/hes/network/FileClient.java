package com.hiido.eagle.hes.network;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FileRegion;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.stream.ChunkedFile;
import io.netty.handler.stream.ChunkedWriteHandler;

public class FileClient {
	
	public void run() throws InterruptedException {
     // Configure the client.  
        EventLoopGroup group = new NioEventLoopGroup();  
        try {  
            Bootstrap b = new Bootstrap();  
            b.group(group)  
             .channel(NioSocketChannel.class)  
             .option(ChannelOption.TCP_NODELAY, true)
             .handler(new ChannelInitializer<SocketChannel>() {  
                 @Override  
                 public void initChannel(SocketChannel ch) throws Exception {  
                     //ch.pipeline().addLast(new FileHandler());  
                	 ch.pipeline().addLast(new ChunkedWriteHandler());//2
                	 ch.pipeline().addLast(new WriteStreamHandler());//3
                 }  
             });  
  
            // Start the client.  
            ChannelFuture f = b.connect("127.0.0.1", 8081).sync();  
  
            Channel channel = f.channel();
            channel.write("/opt/work/data/datafile/test.txt");
            // Wait until the connection is closed.  
            f.channel().closeFuture().sync();  
        } finally {  
            // Shut down the event loop to terminate all threads.  
            group.shutdownGracefully();  
        }  
	}
	
	public static void main(String[] args) {
		try {
			new FileClient().run();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
    @SuppressWarnings("unused")
	private static final class FileHandler extends ChannelOutboundHandlerAdapter {
    	
    	@Override
    	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    		File file = new File(msg.toString());
            if (file.exists()) {
                if (!file.isFile()) {
                    ctx.writeAndFlush("Not a file: " + file + '\n');
                    return;
                }
                ctx.write(file + " " + file.length() + '\n');
                FileInputStream fis = new FileInputStream(file);
                FileRegion region = new DefaultFileRegion(fis.getChannel(), 0, file.length());
                ctx.write(region);
                ctx.writeAndFlush("\n");
                //region.transferTo(target, position)
                fis.close();
            } else {
                ctx.writeAndFlush("File not found: " + file + '\n');
            }
            super.write(ctx, msg, promise);
    	}
    }
    
    public final class WriteStreamHandler extends ChannelOutboundHandlerAdapter {  //4

    	@Override
    	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    		RandomAccessFile file = new RandomAccessFile(msg.toString(), "r");
    		ctx.writeAndFlush(new ChunkedFile(file));
    		super.write(ctx, msg, promise);
    	}
    }

}
