package com.hiido.eagle.hes.md;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.index.translog.TranslogWriter;
import org.junit.Test;

public class TranslogTest {
	
	private static final String ROOT_PATH = "/opt/work/data/es/sealdsp/nodes/0/indices/";
	private static final String TRANSLOG_CODEC = "translog";
	private static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
	private static final int VERSION = VERSION_CHECKPOINTS;
	private static final int BUFFER_SIZE = RamUsageEstimator.NUM_BYTES_INT  // ops
            + RamUsageEstimator.NUM_BYTES_LONG // offset
            + RamUsageEstimator.NUM_BYTES_LONG;// generation
	
	private static final byte LUCENE_CODEC_HEADER_BYTE = 0x3f;
	
	@Test
	public void test01() throws IOException {
		long version = 2L;
		String translogUUID = "IjRyiswCSraDR8e1DDIf_A";
		String indexName = "idx_test_transfer5";
		int shardId = 0;
		String rootTmpPath = ROOT_PATH + File.separator + indexName + File.separator + shardId;
		
		String translogfilePath = rootTmpPath + "/translog/";
        String logPath = translogfilePath + "translog-" + version + ".tlog";
        String ckpPath = translogfilePath + "translog.ckp";
        //String ckpPath = translogfilePath + "translog-" + version + ".ckp";
        
		java.nio.file.Path logFilePath = Paths.get(logPath);
        java.nio.file.Path ckpFilePath = Paths.get(ckpPath);
        
        try (FileChannel logChannel = FileChannel.open(logFilePath, StandardOpenOption.READ)) {
        	InputStreamStreamInput headerStream = new InputStreamStreamInput(Channels.newInputStream(logChannel)); // don't close
            byte b1 = headerStream.readByte();
            if (b1 == LUCENE_CODEC_HEADER_BYTE) {
                byte b2 = headerStream.readByte();
                byte b3 = headerStream.readByte();
                byte b4 = headerStream.readByte();
                int header = ((b1 & 0xFF) << 24) + ((b2 & 0xFF) << 16) + ((b3 & 0xFF) << 8) + ((b4 & 0xFF) << 0);
                if (header != CodecUtil.CODEC_MAGIC) {
                    throw new TranslogCorruptedException("translog looks like version 1 or later, but has corrupted header");
                }
                int versionc = CodecUtil.checkHeaderNoMagic(new InputStreamDataInput(headerStream), TranslogWriter.TRANSLOG_CODEC, 1, Integer.MAX_VALUE);
                switch (versionc) {
                    case TranslogWriter.VERSION_CHECKPOINTS:
                        int len = headerStream.readInt();
                        if (len > logChannel.size()) {
                            throw new TranslogCorruptedException("uuid length can't be larger than the translog");
                        }
                        BytesRef ref = new BytesRef(len);
                        ref.length = len;
                        headerStream.read(ref.bytes, ref.offset, ref.length);
                        System.out.println(">>>>>>>>translogUUID:" + ref.utf8ToString());
                        
                        BytesRef uuidBytes = new BytesRef(translogUUID);
                        if (uuidBytes.bytesEquals(ref) == false) {
                            throw new TranslogCorruptedException("expected shard UUID [" + uuidBytes + "] but got: [" + ref + "] this translog file belongs to a different translog");
                        }
                        break;
                    default:
                        throw new TranslogCorruptedException("No known translog stream version: " + version);
                }
            }
        }
        
        try (InputStream inFile = Files.newInputStream(ckpFilePath)) {
        	InputStreamDataInput in = new InputStreamDataInput(inFile);
            
            long offset = in.readLong();
            int numOps = in.readInt();
            long generation = in.readLong();
            
            System.out.println(">>>>>>>>offset:" + offset);
            System.out.println(">>>>>>>>numOps:" + numOps);
            System.out.println(">>>>>>>>generation:" + generation);
        }
	}

}
