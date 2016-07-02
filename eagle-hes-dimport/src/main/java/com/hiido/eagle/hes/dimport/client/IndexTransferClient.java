package com.hiido.eagle.hes.dimport.client;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hiido.eagle.hes.transfer.ClientBootstrapFactory;
import com.hiido.eagle.hes.transfer.FileTransferClient;
import com.hiido.eagle.hes.transfer.TransferClientConfig;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.stream.ChunkedFile;

public class IndexTransferClient extends FileTransferClient {
	
	private static final Logger LOG = LoggerFactory.getLogger(IndexTransferClient.class);

	public IndexTransferClient(String host, int port, ClientBootstrapFactory clientBootstrapFactory) throws InterruptedException {
		super(host, port, clientBootstrapFactory);
	}

	public static class IndexWriteStreamHandler extends WriteStreamHandler {  
		
		@Override
		protected void transferFile(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			IndexTransferInfo index = (IndexTransferInfo)msg;
			
			File file = new File(index.getFilePath());
			String fileName = file.getName();
			RandomAccessFile accessfile = new RandomAccessFile(file, "r");
			
			ByteBuf buf = ctx.alloc().buffer(32 + index.getIndexName().length() + 32 + 1 + 32 + fileName.length() + 64);
			buf.writeInt(index.getIndexName().length());
			buf.writeBytes(index.getIndexName().getBytes());
			buf.writeInt(index.getShardId());
			buf.writeByte(index.getFileType());
			buf.writeInt(fileName.length());
			buf.writeBytes(fileName.getBytes());  
			buf.writeLong(accessfile.length());
	        ctx.write(buf); 
	        
	        ChunkedFile chunkedFile = new ChunkedFile(accessfile);
	        ctx.writeAndFlush(chunkedFile, promise);
		}
    }
	
	@Override
	public <T> void transfer(T info) throws IOException, InterruptedException {
		
		final IndexTransferInfo index = (IndexTransferInfo)info;
		
		LOG.info("Sending indexName [{}] shardId [{}] file [{}] to server", index.getIndexName(), index.getShardId(), index.getFilePath());
		
		super.transfer(index);
	}
	
	public static IndexTransferClient build(String indexName, String host, int shardId, IndexTransferBootstrapFactory clientBootstrapFactory) throws InterruptedException {
		return new IndexTransferClient(host, TransferClientConfig.SERVER_PORT, clientBootstrapFactory);
	}

	public static class IndexTransferInfo {
		
		private String indexName; 
		private int shardId;
		private byte fileType; 
		private String filePath;
		
		public IndexTransferInfo(String indexName, int shardId, byte fileType, String filePath) {
			this.indexName = indexName;
			this.shardId = shardId;
			this.fileType = fileType;
			this.filePath = filePath;
		}

		public String getIndexName() {
			return indexName;
		}
		public void setIndexName(String indexName) {
			this.indexName = indexName;
		}
		public int getShardId() {
			return shardId;
		}
		public void setShardId(int shardId) {
			this.shardId = shardId;
		}
		public byte getFileType() {
			return fileType;
		}

		public void setFileType(byte fileType) {
			this.fileType = fileType;
		}

		public String getFilePath() {
			return filePath;
		}
		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}
		
	}

}
