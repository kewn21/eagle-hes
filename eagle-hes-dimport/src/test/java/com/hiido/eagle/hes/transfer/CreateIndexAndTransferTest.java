package com.hiido.eagle.hes.transfer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper.CustomDoubleNumericField;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper.DoubleFieldType;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper.CustomFloatNumericField;
import org.elasticsearch.index.mapper.core.FloatFieldType;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper.CustomIntegerNumericField;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper.IntegerFieldType;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.LongFieldMapper.LongFieldType;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper.CustomShortNumericField;
import org.elasticsearch.index.mapper.core.ShortFieldType;
import org.elasticsearch.index.mapper.core.StringFieldMapper.StringFieldType;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.Closeables;
import com.hiido.eagle.hes.dimport.client.IndexTransferBootstrapFactory;
import com.hiido.eagle.hes.dimport.client.IndexTransferClient;
import com.hiido.eagle.hes.dimport.client.IndexTransferClient.IndexTransferInfo;
import com.hiido.eagle.hes.dimport.common.Conts;
import com.hiido.eagle.hes.md.EsMdFactory;

public class CreateIndexAndTransferTest {
	
	private static final Logger LOG = LoggerFactory.getLogger(CreateIndexAndTransferTest.class);
	
	@Test
	public void test() throws InterruptedException, ExecutionException {
		
		HashFunction hashFunction = new Murmur3HashFunction();

		String indexName = "idx_test_transfer_8";
		String indexType = "usertags";
		
		IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
		IndicesExistsResponse existsResp = EsMdFactory.getTransportClient().admin().indices().exists(existsRequest).get();
		if (existsResp.isExists()) {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
			DeleteIndexResponse deleteResp = EsMdFactory.getTransportClient().admin().indices().delete(deleteIndexRequest).get();         
		}
		
		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse createResp = EsMdFactory.getTransportClient().admin().indices().create(createIndexRequest).get();
		
		TransportClient client = EsMdFactory.createTransportClient();
		ClusterStateResponse resp = null;
		IndexRoutingTable routingTable = null;
		int shardNum = 0;
		do {
			resp = client.admin().cluster().prepareState().setIndices(indexName).get();
			routingTable = resp.getState().getRoutingTable().index(indexName);
			if (routingTable.allPrimaryShardsActive()) {
	        	shardNum = routingTable.primaryShardsActive();
			}
	        else {
	        	shardNum = routingTable.primaryShardsUnassigned();
			}
			
			Thread.sleep(500);
		} while (shardNum == 0);
        
        
        ClusterState state = resp.getState();
        ReduceTask[] reduceTasks = new ReduceTask[shardNum];
        for (int i = 0; i < shardNum; i++) {
			String nodeId = state.getRoutingTable().index(indexName).shard(i).primaryShard().currentNodeId();
			String host = state.getNodes().get(nodeId).getName();
			reduceTasks[i] = new ReduceTask(indexName, indexType, host, i, false);
		}
        
        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(indexName);
		CloseIndexResponse closeResp = client.admin().indices().close(closeIndexRequest).get();
		
		System.out.println(">>>>>>shardNum:" + shardNum);
		ExecutorService executorService = Executors.newFixedThreadPool(shardNum);
        for (int i = 0; i < shardNum; i++) {
			executorService.execute(reduceTasks[i]);
		}
		
        String file = "/opt/work/data/datafile/test_data_100.txt";
        
        BufferedReader reader = null;
        try {
        	reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(file))));
        	String line = null;
        	while ((line = reader.readLine()) != null) {
        		int hash = hashFunction.hash(UUID.randomUUID().toString());
    			int shardId = MathUtils.mod(hash, shardNum);
    			reduceTasks[shardId].put(line);
			}
		} catch (Exception e) {
			LOG.error("read file err", e);
		} finally {
			Closeables.closeQuietly(reader);
		}
        
        LOG.info("finish to send data");
        
        final CountDownLatch latch = new CountDownLatch(reduceTasks.length);
        
        for (int i = 0; i < reduceTasks.length; i++) {
        	reduceTasks[i].close(new CloseListener() {
        		public void finished(boolean isClosed) {
        			latch.countDown();
        		}
        	});
		}
        
        //executorService.shutdownNow();
        
        try {
			latch.await();
		} catch (InterruptedException e) {
			LOG.info("Wait to finish err", e);
		}
        
        OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
		OpenIndexResponse openResp = client.admin().indices().open(openIndexRequest).get();
        
        LOG.info("finish to tranfer data");
	}
	
	
	public interface CloseListener {
		void finished(boolean isClosed);
	}
	
	public class ReduceTask implements Runnable {
		
		private final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(100); 
		
		private volatile boolean closed = false;
		
		private IndexWriter indexWriter;
		private long version = 1L;
		private String translogUUID = null;
		
		private String indexType = null;
		private Analyzer indexAnalyzer = null; 
		
		private final int DEFAULT_VALUE = 1;
		private final String DEFAULT_ALL_FIELD_VALUE = "1";
		
		private final IntegerFieldType INTEGER_TYPE = new IntegerFieldType();
		private final StringFieldType STRING_TYPE = new StringFieldType();
		private final LongFieldType LONG_TYPE = new LongFieldType();
		private final DoubleFieldType DOUBLE_TYPE = new DoubleFieldType();
		private final FloatFieldType FLOAT_TYPE = new FloatFieldType();
		private final ShortFieldType SHORT_TYPE = new ShortFieldType();
		
		private final float DEFAULT_BOOST = 0.1F;
		
		private static final String TRANSLOG_CODEC = "translog";
		private static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
		private static final int VERSION = VERSION_CHECKPOINTS;
		private static final int BUFFER_SIZE = RamUsageEstimator.NUM_BYTES_INT  // ops
	            + RamUsageEstimator.NUM_BYTES_LONG // offset
	            + RamUsageEstimator.NUM_BYTES_LONG;// generation
		
		private static final String ROOT_PATH = "/opt/work/data/indexdata";
		
		private String indexName;
		private int shardId;
		private String host;
		private boolean autoGenerateId;
		private String rootTmpPath;
		
		private static final String COL_PREX = "col_";
		
		private CloseListener closeListener;
		
		public ReduceTask(String indexName, String indexType, String host, int shardId, boolean autoGenerateId) {
			
			STRING_TYPE.setHasDocValues(true);
			
			INTEGER_TYPE.setNumericPrecisionStep(NumberFieldMapper.Defaults.PRECISION_STEP_32_BIT);
			INTEGER_TYPE.setHasDocValues(true);
			
			FLOAT_TYPE.setNumericPrecisionStep(NumberFieldMapper.Defaults.PRECISION_STEP_32_BIT);
			FLOAT_TYPE.setHasDocValues(true);
			
			LONG_TYPE.setNumericPrecisionStep(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT);
			LONG_TYPE.setHasDocValues(true);
			
			DOUBLE_TYPE.setNumericPrecisionStep(NumberFieldMapper.Defaults.PRECISION_STEP_64_BIT);
			DOUBLE_TYPE.setHasDocValues(true);
			
			SHORT_TYPE.setNumericPrecisionStep(ShortFieldMapper.DEFAULT_PRECISION_STEP);
			SHORT_TYPE.setHasDocValues(true);
			
			
			this.indexName = indexName;
			this.shardId = shardId;
			this.host = host;
			this.indexType = indexType;
			this.autoGenerateId = autoGenerateId;
			
			rootTmpPath = ROOT_PATH + File.separator + indexName + File.separator + shardId;
			
			try {
				indexAnalyzer = new StandardAnalyzer();
			    IndexWriterConfig indexConfig = new IndexWriterConfig(indexAnalyzer);
			    /*((TieredMergePolicy)indexConfig.getMergePolicy()).setFloorSegmentMB(20);
			    config.setRAMBufferSizeMB(10);*/
			    
			    translogUUID = org.elasticsearch.common.Strings.randomBase64UUID();
			    
			    System.out.println(">>>>>>>>translogUUID:" + translogUUID);
			    
			    String indexPath = rootTmpPath + "/index";
			    indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath).toPath()), indexConfig);
			} catch (Exception e) {
				LOG.error("init indexWriter err", e);
			}
		}
		
		public void close(CloseListener listener) {
			closed = true;
			closeListener = listener;
		}
		
		public void put(String line) {
			try {
				queue.put(line);
			} catch (InterruptedException e) {
				LOG.error("put err", e);
			}
		}
		
		private void clean() {
			LOG.info("Clean index {} shard {}", indexName, shardId);
			
			IndexTransferBootstrapFactory clientBootstrapFactory = new IndexTransferBootstrapFactory();
			
			try {
				Map<String, String> commitData = new HashMap<>(2);
		        commitData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(version));
		        commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
		        indexWriter.setCommitData(commitData);
				indexWriter.close();
				
				
				final BytesRef ref = new BytesRef(translogUUID);
		        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC) + ref.length + RamUsageEstimator.NUM_BYTES_INT;
		       
		        String translogfilePath = rootTmpPath + "/translog/";
		        String logPath = translogfilePath + "translog-" + version + ".tlog";
		        String ckpPath = translogfilePath + "translog.ckp";
		        
		        new File(translogfilePath).mkdir();
		        new File(logPath).createNewFile();
		        new File(ckpPath).createNewFile();
		        
		        java.nio.file.Path logFilePath = Paths.get(logPath);
		        java.nio.file.Path ckpFilePath = Paths.get(ckpPath);
		        
		        try (FileChannel logChannel = FileChannel.open(logFilePath, StandardOpenOption.WRITE)) {
		            final OutputStreamDataOutput logOut = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(logChannel));
		            CodecUtil.writeHeader(logOut, TRANSLOG_CODEC, VERSION);
		            logOut.writeInt(ref.length);
		            logOut.writeBytes(ref.bytes, ref.offset, ref.length);
		            logChannel.force(false);
	        	}
	            
	        	try (FileChannel ckpChannel = FileChannel.open(ckpFilePath, StandardOpenOption.WRITE)) {
		            byte[] buffer = new byte[BUFFER_SIZE];
		            final ByteArrayDataOutput ckpOut = new ByteArrayDataOutput(buffer);
		            ckpOut.writeLong(headerLength);
		            ckpOut.writeInt(0);
		            ckpOut.writeLong(version);
		            Channels.writeToChannel(buffer, ckpChannel);
		            ckpChannel.force(false);
	        	}
	        	
				
				LOG.info("Transfer file [{}] of index {} shard {}", rootTmpPath, indexName, shardId);
				
				IndexTransferClient client = null;
				IndexTransferInfo pojo = null;
				
				File localTmpPath = new File(rootTmpPath);
				
				for (File localFolder : localTmpPath.listFiles()) {
					if (localFolder.isDirectory() 
							&& (Conts.FILE_TYPE_INDEX_FOLDER.equals(localFolder.getName())
								|| Conts.FILE_TYPE_TRANSLOG_FOLDER.equals(localFolder.getName()))
								) {
						
						byte fileType = Conts.FILE_TYPE_INDEX_FOLDER.equals(localFolder.getName()) ? Conts.FILE_TYPE_INDEX : Conts.FILE_TYPE_TRANSLOG;
						
						for (File file : localFolder.listFiles()) {
							/*if (!file.getPath().endsWith("/_0.si")) {
								continue;
							}*/
							try {
								client = IndexTransferClient.build(indexName, host, shardId, clientBootstrapFactory);
								pojo = new IndexTransferInfo(indexName, shardId, fileType, file.getPath());
								client.transfer(pojo);
							} catch (Exception e) {
								LOG.error("client transfer file [{}] err", file.getPath(), e);
							} finally {
								try {
									Closeables.close(client, true);
								} catch (Exception e2) {
									LOG.error("close client err", e2);
								}
							}
						}
					}
				}
			} catch (Exception e) {
				LOG.error("client transfer folder [{}] err", rootTmpPath, e);
			} finally {
				clientBootstrapFactory.shutdown();
				
				if (closeListener != null) {
					closeListener.finished(closed);
				}
			}
		}

		@Override
		public void run() {
			while (true) {
				if (closed && queue.isEmpty()) {
					clean();
					break;
				}
				
				try {
					String line = queue.poll(20, TimeUnit.NANOSECONDS);
					if (line != null) {
						String[] cells = line.split(delim);
						if (cells.length >= 1) {
							String uid = null;
							int startIndex = 0;
							if (!autoGenerateId) {
								uid = cells[0];
								//uid = org.elasticsearch.common.Strings.randomBase64UUID();
								//uid = "1";
								System.out.println(uid);
								startIndex = 1;
								if (cells.length == 1) {
									continue;
								}
							}
							else {
								uid = org.elasticsearch.common.Strings.randomBase64UUID();
								startIndex = 0;
							}
							
							Document doc = new Document();
							
							XContentBuilder xbuilder = XContentFactory.jsonBuilder();
							xbuilder.startObject();
							
							AllEntries allEntries = new AllEntries();
							List<Field> fields = Lists.newArrayList();
							
							for (int j = startIndex; j < cells.length; j++) {
								String[] cols = cells[j].split(",");
								for (int i = 0; i < cols.length; i++) {
									String[] parts = cols[i].split(":");
									String colName = null;
									Object sourceValue = null;
									String allFieldColValue = null;
									
									if (parts.length == 1) {
										colName = COL_PREX + parts[0];
										allFieldColValue = DEFAULT_ALL_FIELD_VALUE;
										sourceValue = DEFAULT_VALUE;
										MappedFieldType fieldType = INTEGER_TYPE;
										fieldType.setNames(new Names(colName));
										
										if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
											Field field = new CustomIntegerNumericField(DEFAULT_VALUE, fieldType);
											//field.setBoost(DEFAULT_BOOST);
											fields.add(field);
										}
										if (fieldType.hasDocValues()) {
											addDocValue(fieldType, fields, DEFAULT_VALUE);
										}
									}
									else if (parts.length == 2) {
										colName = parts[0];
										String val = parts[1];
										allFieldColValue = val;
										sourceValue = val;
										MappedFieldType fieldType = STRING_TYPE;
										fieldType.setNames(new Names(colName));
										
										if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
											Field field = new Field(colName, val, fieldType);
											//field.setBoost(DEFAULT_BOOST);
											fields.add(field);
										}
										if (fieldType.hasDocValues()) {
								            fields.add(new SortedSetDocValuesField(fieldType.names().indexName(), new BytesRef(val)));
								        }
									}
									else if (parts.length >= 3) {
										colName = parts[0];
										allFieldColValue = parts[1];
										String valueType = parts[2];
										
										if (IntegerFieldMapper.CONTENT_TYPE.equalsIgnoreCase(valueType)) {
											int val = Integer.valueOf(parts[1]);
											NumberFieldType fieldType = INTEGER_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new CustomIntegerNumericField(val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
												addDocValue(fieldType, fields, val);
											}
										}
										else if (DoubleFieldMapper.CONTENT_TYPE.equalsIgnoreCase(valueType)) {
											double val = Double.valueOf(parts[1]);
											NumberFieldType fieldType = DOUBLE_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new CustomDoubleNumericField(val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
												addDocValue(fieldType, fields, NumericUtils.doubleToSortableLong(val));
											}
										}
										else if (LongFieldMapper.CONTENT_TYPE.equalsIgnoreCase(valueType)) {
											long val = Long.valueOf(parts[1]);
											NumberFieldType fieldType = LONG_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new CustomLongNumericField(val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
												addDocValue(fieldType, fields, val);
											}
										}
										else if (FloatFieldMapper.CONTENT_TYPE.equalsIgnoreCase(valueType)) {
											float val = Float.valueOf(parts[1]);
											NumberFieldType fieldType = FLOAT_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new CustomFloatNumericField(val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
												addDocValue(fieldType, fields, NumericUtils.floatToSortableInt(val));
											}
										}
										else if (ShortFieldMapper.CONTENT_TYPE.equalsIgnoreCase(valueType)) {
											short val = Short.valueOf(parts[1]);
											NumberFieldType fieldType = SHORT_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new CustomShortNumericField(val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
												addDocValue(fieldType, fields, val);
											}
										}
										else {
											String val = parts[1];
											MappedFieldType fieldType = STRING_TYPE;
											fieldType.setNames(new Names(colName));
											sourceValue = val;
											
											if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
												Field field = new Field(colName, val, fieldType);
												//field.setBoost(DEFAULT_BOOST);
												fields.add(field);
											}
											if (fieldType.hasDocValues()) {
									            fields.add(new SortedSetDocValuesField(fieldType.names().indexName(), new BytesRef(val)));
									        }
										}
									}
									
									xbuilder.field(colName, sourceValue);
									allEntries.addText(colName, allFieldColValue, DEFAULT_BOOST);
								}
							}
									
							xbuilder.endObject();
							
							BytesReference source = xbuilder.bytes();
							doc.add(new StoredField(SourceFieldMapper.Defaults.FIELD_TYPE.names().indexName(), source.array(), source.arrayOffset(), source.length()));
							
							doc.add(new Field(TypeFieldMapper.Defaults.FIELD_TYPE.names().indexName(), indexType, TypeFieldMapper.Defaults.FIELD_TYPE));
					        if (TypeFieldMapper.Defaults.FIELD_TYPE.hasDocValues()) {
					        	doc.add(new SortedSetDocValuesField(TypeFieldMapper.Defaults.FIELD_TYPE.names().indexName(), new BytesRef(indexType)));
					        }
					        
					        doc.add(new Field(UidFieldMapper.Defaults.FIELD_TYPE.names().indexName(), Uid.createUid(new StringBuilder(), indexType, uid), UidFieldMapper.Defaults.FIELD_TYPE));
					        
					        doc.add(new NumericDocValuesField(VersionFieldMapper.Defaults.FIELD_TYPE.names().indexName(), version));
					        
					        for (Field field : fields) {
								doc.add(field);
							}
					        
					        Analyzer allEntriesAnalyzer = new FieldNameAnalyzer(indexAnalyzer);
					        doc.add(new AllField(AllFieldMapper.Defaults.FIELD_TYPE.names().indexName(), allEntries, allEntriesAnalyzer, AllFieldMapper.Defaults.FIELD_TYPE));
					        
					        List<String> paths = Lists.newArrayList();
					        for (IndexableField field : doc) {
					        	paths.add(field.name());
							}
					        
					        for (String path : paths) {
					        	for (String fieldName : extractFieldNames(path)) {
						        	if (FieldNamesFieldMapper.Defaults.FIELD_TYPE.indexOptions() != IndexOptions.NONE || FieldNamesFieldMapper.Defaults.FIELD_TYPE.stored()) {
						            	doc.add(new Field(FieldNamesFieldMapper.Defaults.FIELD_TYPE.names().indexName(), fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
						            }
					        	}
							}
					        
					        indexWriter.addDocument(doc);
						}
					}
				} catch (Exception e) {
					LOG.error("Create doc err", e);
				}
			}
		}
	}
	
	protected final void addDocValue(MappedFieldType fieldType, List<Field> fields, long value) {
		fields.add(new SortedNumericDocValuesField(fieldType.names().indexName(), value));
    }
	
	private String delim = "\t";
	
	static Iterable<String> extractFieldNames(final String fullPath) {
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new UnmodifiableIterator<String>() {

                    int endIndex = nextEndIndex(0);

                    private int nextEndIndex(int index) {
                        while (index < fullPath.length() && fullPath.charAt(index) != '.') {
                            index += 1;
                        }
                        return index;
                    }

                    @Override
                    public boolean hasNext() {
                        return endIndex <= fullPath.length();
                    }

                    @Override
                    public String next() {
                        final String result = fullPath.substring(0, endIndex);
                        endIndex = nextEndIndex(endIndex + 1);
                        return result;
                    }

                };
            }
        };
    }

}
