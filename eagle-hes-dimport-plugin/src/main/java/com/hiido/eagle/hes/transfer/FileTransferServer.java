/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.hiido.eagle.hes.transfer;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import com.hiido.eagle.hes.dimport.common.Conts;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ReferenceCountUtil;

/**
 * Server that accept the path of a file an echo back its content.
 */
public class FileTransferServer {
	
	ESLogger logger = Loggers.getLogger(FileTransferServer.class);
	
	private String host;
    private int port;
    private int workNum;
    
    public FileTransferServer(int port, int workNum) {
		this.port = port;
		this.workNum = workNum;
    }

    public FileTransferServer(String host, int port, int workNum) {
    	this.host = host;
        this.port = port;
        this.workNum = workNum;
    }

    public void start() throws Exception {
    	
    	logger.info("Start server at host:{} port:{}, the number of work:{}", host, port, workNum);
    	
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup(workNum);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .option(ChannelOption.SO_BACKLOG, 100)
             .handler(new LoggingHandler(LogLevel.INFO))
             .childHandler(new ChannelInitializer<SocketChannel>() {
            	 
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new FileHandler());
                 }
             });

            ChannelFuture f = null;
            if (host == null) {
        		f = b.bind(port).sync();
			}
            else {
            	f = b.bind(host, port).sync();
            }
            
            logger.info("Server bound host:{} port:{} successfully", host, port);
            
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public class FileHandler extends ChannelInboundHandlerAdapter {  
    	
    	private long startTime;
    	private byte ret = Conts.TRANSFER_SUC;
    	
    	private FileStoreInfo fileStoreInfo = null;
    	
    	@Override
    	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			startTime = System.currentTimeMillis();
    		try {
    			if (fileStoreInfo == null) {
    				fileStoreInfo = createAndStoreFile(ctx, msg);
				}
    			else {
    				storeFile(ctx, msg, fileStoreInfo);
				}
    		} catch (Exception e) {
    			ret = Conts.TRANSFER_ERR;
    			
    			if (fileStoreInfo != null) {
    				logger.error("Handler the file [{}] err", fileStoreInfo.getFileName(), e);
				}
    			else {
    				logger.error("Handler msg [{}] err", msg, e);
    			}
    		} finally {
    	        ReferenceCountUtil.release(msg); 
    	        closeFile(ctx);
    	    }
    	}
    	
    	private void closeFile(ChannelHandlerContext ctx) {
    		try {
    			if (fileStoreInfo != null) {
    				if (fileStoreInfo.getCurrFileSize() >= fileStoreInfo.getFileSize()) {
    					long endTime = System.currentTimeMillis();
    					logger.info("The file [{}] size [{}b] be wirten to local disk cost time : {} ms,so the local file should be closed"
        					, fileStoreInfo.getFileName(), fileStoreInfo.getFileSize(), endTime - startTime);
        			
        				fileStoreInfo.getFileChannel().force(true);
        				fileStoreInfo.getFileChannel().close();

            	        askClient(ctx);
    				} 
    			}
			} catch (Exception e) {
				ret = Conts.TRANSFER_ERR;
				logger.error("Close the file [{}] err", fileStoreInfo.getFile(), e);
			} finally {
				if (ret == Conts.TRANSFER_ERR) {
					askClient(ctx);
				}
			}
		}
    	
    	private void askClient(ChannelHandlerContext ctx) {
    		ByteBuf buf = ctx.alloc().buffer(1);
    		buf.writeByte(ret);
    		ctx.writeAndFlush(buf);
    	}
    }
    
    protected FileStoreInfo createAndStoreFile(ChannelHandlerContext ctx, Object msg) throws Exception {
    	ByteBuf in = (ByteBuf) msg;
    	int filePathSize = in.readInt();
		String fileName = new String(in.readBytes(filePathSize).array());
		long fileSize = in.readLong();
		
		String localPath = TransferServerConfig.class.getResource("/").getPath() + fileName;
		logger.info("Start to handler the file [{}] with size [{}b] which be writen to local disk [{}]", fileName, fileSize, localPath);
		
		long currFileSize = in.readableBytes();
		ByteBuffer buf = ByteBuffer.allocate((int)currFileSize);
		in.readBytes(buf);

		RandomAccessFile accessfile = new RandomAccessFile(localPath, "rw");
		FileChannel fileChannel = accessfile.getChannel();
		fileChannel.write(buf);
		return new FileStoreInfo(accessfile, fileChannel, fileName, fileSize, currFileSize);
	}
    
    protected void storeFile(ChannelHandlerContext ctx, Object msg, FileStoreInfo fileStoreInfo) throws Exception {
    	ByteBuf in = (ByteBuf) msg;
    	fileStoreInfo.incrCurrFileSize(in.readableBytes());
    	fileStoreInfo.getFileChannel().write(in.nioBuffer());
    }
    
    public class FileStoreInfo {
    	private RandomAccessFile file;
		private FileChannel fileChannel;
    	private String fileName;
    	private long fileSize;
    	private long currFileSize;
    	
    	public FileStoreInfo(RandomAccessFile file, FileChannel fileChannel, String fileName, long fileSize, long currFileSize) {
			this.file = file;
			this.fileChannel = fileChannel;
			this.fileName = fileName;
			this.fileSize = fileSize;
			this.currFileSize = currFileSize;
		}
    	
    	public long incrCurrFileSize(long currFileSize) {
			this.currFileSize += currFileSize;
			return this.currFileSize;
		}
    	
    	public RandomAccessFile getFile() {
			return file;
		}

		public void setFile(RandomAccessFile file) {
			this.file = file;
		}

		public FileChannel getFileChannel() {
			return fileChannel;
		}

		public void setFileChannel(FileChannel fileChannel) {
			this.fileChannel = fileChannel;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public long getFileSize() {
			return fileSize;
		}

		public void setFileSize(long fileSize) {
			this.fileSize = fileSize;
		}

		public long getCurrFileSize() {
			return currFileSize;
		}

		public void setCurrFileSize(long currFileSize) {
			this.currFileSize = currFileSize;
		}
    }

    
    public static void main(String[] args) {
        try {
        	FileTransferServer server = new FileTransferServer("127.0.0.1", 8081, 100);
			server.start();
		} catch (Exception e) {
		}
    }
}
