package com.hiido.eagle.hes.dimport.server;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.env.Environment;

import com.hiido.eagle.hes.dimport.common.Conts;
import com.hiido.eagle.hes.transfer.FileTransferServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public class IndexTransferServer extends FileTransferServer {
	
	ESLogger logger = Loggers.getLogger(IndexTransferServer.class);
	
	private final Environment environment;
	
	@Inject
	public IndexTransferServer(Environment environment) {
		super(environment.settings().get("dimport.host")
				, environment.settings().getAsInt("dimport.port", 2184)
				, environment.settings().getAsInt("dimport.work.num", 500));
		this.environment = environment;
	}
	
	@Override
	protected FileStoreInfo createAndStoreFile(ChannelHandlerContext ctx, Object msg) throws Exception {
		ByteBuf in = (ByteBuf) msg;
		
		int indexNameSize = in.readInt();
		String indexName = new String(in.readBytes(indexNameSize).array());
		int shardId = in.readInt();
		byte fileType = in.readByte();
    	int filePathSize = in.readInt();
		String fileName = new String(in.readBytes(filePathSize).array());
		long fileSize = in.readLong();
		
		String filePath = getShardFilePath(indexName, shardId);
		if (filePath == null) {
			throw new Exception("Can not find the file path of index [" + indexName + "] shard [" + shardId + "]");
		}
		
		String folderName = null;
		if (Conts.FILE_TYPE_INDEX == fileType) {
			folderName = Conts.FILE_TYPE_INDEX_FOLDER;
		}
		else if (Conts.FILE_TYPE_TRANSLOG == fileType) {
			folderName = Conts.FILE_TYPE_TRANSLOG_FOLDER;
		}
		
		if (folderName == null) {
			throw new Exception("The fileType byte to decide the folderName which be not definited");
		}
		
		String localPath = filePath + File.separator + folderName + File.separator + fileName;
		logger.info("Start to handler the file [{}] size [{}b] which be writen to local disk [{}]", fileName, fileSize, localPath);
		
		RandomAccessFile accessfile = new RandomAccessFile(localPath, "rw");
		
		FileChannel fileChannel = accessfile.getChannel();
		
		long currFileSize = in.readableBytes();
		
		ByteBuffer buf = ByteBuffer.allocate((int)currFileSize);
		in.readBytes(buf);
		
		buf.flip();
		fileChannel.write(buf);
		
		return new FileStoreInfo(accessfile, fileChannel, fileName, fileSize, currFileSize);
	}
	
	public String getShardFilePath(String indexName, int shardId) {
		for (Path filePath : environment.dataFiles()) {
			File file = new File(filePath 
					+ File.separator + environment.settings().get("cluster.name") 
					+ File.separator + "nodes/0/indices"
					+ File.separator + indexName
					+ File.separator + shardId + "/");
			
			if (file.exists()) {
				return file.getPath();
			}
		} 
		return null;
	}

}
