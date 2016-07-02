package com.hiido.eagle.hes.dimport.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.math.MathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
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
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;
import org.elasticsearch.index.translog.Translog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.hiido.eagle.hes.dimport.common.Conts;
import com.hiido.eagle.hes.dimport.mr.output.ClientTransferOutputCommitter;
import com.hiido.eagle.hes.dimport.mr.output.ClientTransferOutputFormat;
import com.hiido.eagle.hes.md.EsMdFactory;

public class HesImportJob extends Configured implements Tool {
	
	protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

	public static final String NAME = HesImportJob.class.getSimpleName();
	
	public static class CreateIndexMapper extends
			Mapper<LongWritable, Text, IndexKey, Text> {
		
		protected final IndexKey key = new IndexKey();
		protected final Text text = new Text();
		protected boolean autoGenerateId;
		protected String delim = "\t";
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, IndexKey, Text>.Context context)
				throws IOException, InterruptedException {
			delim = context.getConfiguration().get("delim", "\t");
			autoGenerateId = context.getConfiguration().getBoolean(Conts.PRO_AUTO_GENERATE_ID, false);
		}
		
		@Override
		public void map(LongWritable offset, Text value
				, Mapper<LongWritable, Text, IndexKey, Text>.Context context)
				throws IOException, InterruptedException {
			
			String[] cells = value.toString().split(delim);
			if (cells.length >= 1) {
				if (!autoGenerateId) {
					if (cells.length > 1) {
						StringBuilder sb = new StringBuilder();
						for (int i = 1; i < cells.length; i++) {
							if (sb.length() > 0) {
								sb.append(delim);
							}
							sb.append(cells[i]);
						}
						
						key.setKey(cells[0]);
						text.set(sb.toString());
						context.write(key, text);
					}
				}
				else {
					key.setKey(org.elasticsearch.common.Strings.randomBase64UUID());
					context.write(key, value);
				}
			}
		}
	}
	
	
	public static class CreateIndexReducer extends
		Reducer<IndexKey, Text, NullWritable, NullWritable> {
		
		protected IndexWriter indexWriter = null;
		protected long version = 1L;
		protected String translogUUID = null;
		
		protected File localTmpPath = null;
		protected String indexName = null;
		protected String indexType = null;
		protected Analyzer indexAnalyzer = null; 
		
		protected final int DEFAULT_VALUE = 1;
		//protected final String DEFAULT_ALL_FIELD_VALUE = "1";
		//protected final float DEFAULT_BOOST = 0.1F;
		
		protected final IntegerFieldType INTEGER_TYPE = new IntegerFieldType();
		protected final StringFieldType STRING_TYPE = new StringFieldType();
		protected final LongFieldType LONG_TYPE = new LongFieldType();
		protected final DoubleFieldType DOUBLE_TYPE = new DoubleFieldType();
		protected final FloatFieldType FLOAT_TYPE = new FloatFieldType();
		protected final ShortFieldType SHORT_TYPE = new ShortFieldType();
		
		protected static final String TRANSLOG_CODEC = "translog";
		protected static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
		protected static final int VERSION = VERSION_CHECKPOINTS;
		protected static final int BUFFER_SIZE = RamUsageEstimator.NUM_BYTES_INT  // ops
	            + RamUsageEstimator.NUM_BYTES_LONG // offset
	            + RamUsageEstimator.NUM_BYTES_LONG;// generation
		
		protected static final String COL_PREX = "col_";
		protected String delim = "\t";
		
		@Override
		protected void setup(Context context) throws IOException {
		    
			indexName = context.getConfiguration().get(Conts.PRO_INDEX_NAME);
		    indexType = context.getConfiguration().get(Conts.PRO_INDEX_TYPE);
			delim = context.getConfiguration().get("delim", "\t");
			
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
			
			
			localTmpPath = ((ClientTransferOutputCommitter)context.getOutputCommitter()).getLocalTmpPath();
		    
		    indexAnalyzer = new StandardAnalyzer();
		    IndexWriterConfig indexConfig = new IndexWriterConfig(indexAnalyzer);
		    /*((TieredMergePolicy)indexConfig.getMergePolicy()).setFloorSegmentMB(20);
		    indexConfig.setRAMBufferSizeMB(200);
		    indexConfig.setMaxBufferedDocs(20000);*/
		    
		    translogUUID = org.elasticsearch.common.Strings.randomBase64UUID();

		    String indexPath = localTmpPath.getPath() + "/index";
		    indexWriter = new IndexWriter(FSDirectory.open(new File(indexPath).toPath()), indexConfig);
		}
		
		@Override
		protected void reduce(IndexKey key, Iterable<Text> values
				, Reducer<IndexKey, Text, NullWritable, NullWritable>.Context context) throws IOException, InterruptedException {
		
			for (Text text : values) {
				String[] cells = text.toString().split(delim);
				if (cells.length >= 1) {
					String uid = key.getKey();
					
					Document doc = new Document();
					XContentBuilder xbuilder = XContentFactory.jsonBuilder();
					xbuilder.startObject();
					//AllEntries allEntries = new AllEntries();
					List<Field> fields = Lists.newArrayList();
					
					for (int j = 0; j < cells.length; j++) {
						String[] cols = cells[j].split(",");
						for (int i = 0; i < cols.length; i++) {
							String[] parts = cols[i].split(":");
							String colName = null;
							Object sourceValue = null;
							//String allFieldColValue = null;
							
							if (parts.length == 1) { //colvalue
								colName = COL_PREX + parts[0];
								//allFieldColValue = DEFAULT_ALL_FIELD_VALUE;
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
							else if (parts.length == 2) { //colname:colvalue
								colName = parts[0];
								String val = parts[1];
								//allFieldColValue = val;
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
							else if (parts.length >= 3) { //colname:colvalue:coltype
								colName = parts[0];
								//allFieldColValue = parts[1];
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
							//allEntries.addText(colName, allFieldColValue, DEFAULT_BOOST);
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
			        
			        //Analyzer allEntriesAnalyzer = new FieldNameAnalyzer(indexAnalyzer);
			        //doc.add(new AllField(AllFieldMapper.Defaults.FIELD_TYPE.names().indexName(), allEntries, allEntriesAnalyzer, AllFieldMapper.Defaults.FIELD_TYPE));
			        
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
			        //indexWriter.commit();
				}
			}
		}
		
		@Override
		protected void cleanup(
				Reducer<IndexKey, Text, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			Map<String, String> commitData = new HashMap<>(2);
	        commitData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(version));
	        commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
	        indexWriter.setCommitData(commitData);
			indexWriter.close();
		    
		    final BytesRef ref = new BytesRef(translogUUID);
	        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC) + ref.length + RamUsageEstimator.NUM_BYTES_INT;
	       
	        String translogfilePath = localTmpPath.getPath() + "/translog/";
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
		}	
		
		protected void addDocValue(MappedFieldType fieldType, List<Field> fields, long value) {
			fields.add(new SortedNumericDocValuesField(fieldType.names().indexName(), value));
	    }
		
		protected Iterable<String> extractFieldNames(final String fullPath) {
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

	
	public static class IndexKey implements
			WritableComparable<IndexKey> {
		
		private String key;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
		
		public IndexKey() {
		}
		
		public void set(String key) {
			this.setKey(key);
		}
		
		public void readFields(DataInput in) throws IOException {
			key = in.readUTF();
		}
		
		public void write(DataOutput out) throws IOException {
			out.writeUTF(key);
		}
		
		public int compareTo(IndexKey o) {
			if (!o.key.equals(key)) {
				return o.key.compareTo(key);
			} else {
				return 0;
			}
		}
		
		public int hashCode() {
			return key.hashCode();
		}
		
		public boolean equals(Object right) {
			if (right == null)
				return false;
			if (this == right)
				return true;
			if (right instanceof IndexKey) {
				IndexKey r = (IndexKey) right;
				return r.key.equals(key);
			} else {
				return false;
			}
		}
	}
		
	public static class IndexPartitioner extends
		Partitioner<IndexKey, Text> {
		
		final HashFunction hashFunction = new Murmur3HashFunction();
	
		public int getPartition(IndexKey key, Text value, int numPartitions) {
			int hash = hashFunction.hash(key.getKey());
			int shardId = MathUtils.mod(hash, numPartitions);
			return shardId;
		}
	}

		
	@Override
	public int run(String[] arg) throws Exception {

		if (arg.length < 2) {
			LOG.error("No enough args for inputfilepath and onputfilepath");
			System.exit(-1);
		}
		
		Configuration conf = getConf();
		if (arg.length >= 3) {
			conf.set("delim", arg[2]);
		}

		String indexName = conf.get(Conts.PRO_INDEX_NAME);
		if (Strings.isNullOrEmpty(indexName)) {
			LOG.error("No found property [index.name]");
			System.exit(-1);
		}
		
		beforeInitIndexJobConfg(indexName);
		
		int shardNum = initIndexJobConfg(indexName);
		
		afterInitIndexJobConfg(indexName);

		Job job = initJob(shardNum, arg);

		int res = job.waitForCompletion(true) ? 0 : 1;
		
		afterJobFinish(res, indexName);
		
		return res;
	}
	
	protected void beforeInitIndexJobConfg(String indexName) throws InterruptedException, ExecutionException {
		//check index exists or not
		IndicesExistsRequest existsRequest = new IndicesExistsRequest(indexName);
		IndicesExistsResponse existsResp = EsMdFactory.getTransportClient().admin().indices().exists(existsRequest).get();
		if (existsResp.isExists()) {
			//if exists delete it first
			DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
			DeleteIndexResponse deleteResp = EsMdFactory.getTransportClient().admin().indices().delete(deleteRequest).get(); 
			if (!deleteResp.isAcknowledged()) {
				throw new IllegalStateException("delete index [" + indexName + "] fail");
			}
		}
	}
	
	protected int initIndexJobConfg(String indexName) throws InterruptedException, ExecutionException {
		Configuration conf = getConf();
		
		//create index
		CreateIndexRequest createRequest = new CreateIndexRequest(indexName);
		CreateIndexResponse createResp = EsMdFactory.getTransportClient().admin().indices().create(createRequest).get();
		if (!createResp.isAcknowledged()) {
			throw new IllegalStateException("create index [" + indexName + "] fail");
		}
		
		ClusterStateResponse stateResp = null;
		boolean allShardsActived = false;
		int checkTimes = 0;
		int maxCheckTimes = 60;
		do {
			if (checkTimes < maxCheckTimes) {
				Thread.sleep(1000);
			}
			
			stateResp = EsMdFactory.getTransportClient().admin().cluster().prepareState().setIndices(indexName).get();
			ClusterState state = stateResp.getState();
			IndexRoutingTable routingTable = state.getRoutingTable().index(indexName);
			
			if (routingTable != null) {
				int shardNum = state.getRoutingTable().index(indexName).shards().size();
				if (shardNum > 0) {
					allShardsActived = true;
					for (int i = 0; i < shardNum; i++) {
						IndexShardRoutingTable shardRoutingTable = state.getRoutingTable().index(indexName).shard(i);
						List<ShardRouting> replicaShards =  shardRoutingTable.replicaShards();
						if (replicaShards.size() > 0) {
							List<String> replicaHosts = Lists.newArrayList();
							for (ShardRouting replicaShard : replicaShards) {
								if (replicaShard.assignedToNode() && replicaShard.active()) {
									String nodeId = replicaShard.currentNodeId();
									String host = state.getNodes().get(nodeId).getName();
									replicaHosts.add(host);
								}
							}
							
							if (replicaHosts.size() == 0) {
								allShardsActived = false;
								break;
							}
						}

						ShardRouting primaryShard = shardRoutingTable.primaryShard();
						if (!(primaryShard.assignedToNode() && primaryShard.active())) {
							allShardsActived = false;
							break;
						}
					}
				}
			}
			
			checkTimes++;
		} while (!allShardsActived);
		
		if (!allShardsActived) {
			throw new IllegalStateException("can not get metadata of index [" + indexName + "] after " + maxCheckTimes + " times check");
		}

		
        ClusterState state = stateResp.getState();
		int shardNum = state.getRoutingTable().index(indexName).shards().size();
        if (shardNum == 0) {
        	throw new IllegalStateException("index [" + indexName + "] has not any shards");
		}
        
        LOG.info("found {} shards of index after {} times check", shardNum, indexName, checkTimes);
        
		for (int i = 0; i < shardNum; i++) {
			IndexShardRoutingTable shardRoutingTable = state.getRoutingTable().index(indexName).shard(i);
			
			ShardRouting primaryShard = shardRoutingTable.primaryShard();
			List<ShardRouting> replicaShards =  shardRoutingTable.replicaShards();
			if (replicaShards.size() > 0) {
				List<String> replicaHosts = Lists.newArrayList();
				for (ShardRouting replicaShard : replicaShards) {
					if (replicaShard.assignedToNode() && replicaShard.active()) {
						String nodeId = replicaShard.currentNodeId();
						String host = state.getNodes().get(nodeId).getName();
						replicaHosts.add(host);
					}
				}
				
				if (replicaHosts.size() == 0) {
					throw new IllegalStateException("index [" + indexName + "] shard [" + i + "] has not any active replica shards");
				}
				
				conf.set("shard" + i + ".replica.host", Joiner.on(",").join(replicaHosts));
				
				LOG.info("shard{}.replica.host:{}", i, Joiner.on(",").join(replicaHosts));
			}
			
			if (primaryShard.assignedToNode() && primaryShard.active()) {
				String nodeId = primaryShard.currentNodeId();
				String host = state.getNodes().get(nodeId).getName();
				conf.set("shard" + i + ".primary.host", host);
				
				LOG.info("shard{}.primary.host:{}", i, host);
			}
			else {
				throw new IllegalStateException("index [" + indexName + "] shard [" + shardNum + "] has not primary shard");
			}
		}
		
		return shardNum;
	}
	
	protected void afterInitIndexJobConfg(String indexName) throws InterruptedException, ExecutionException {
		//close before creating index's data
		CloseIndexRequest closeRequest = new CloseIndexRequest(indexName);
		CloseIndexResponse closeResp = EsMdFactory.getTransportClient().admin().indices().close(closeRequest).get();
		if (!closeResp.isAcknowledged()) {
			throw new IllegalStateException("close index [" + indexName + "] fail");
		}
	}
	
	protected Job initJob(int shardNum, String[] arg) throws IOException {
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf, HesImportJob.NAME);
		job.setJarByClass(HesImportJob.class);
		job.setMapperClass(CreateIndexMapper.class);
		job.setReducerClass(CreateIndexReducer.class);
		job.setNumReduceTasks(shardNum);
		job.setMapOutputKeyClass(IndexKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setPartitionerClass(IndexPartitioner.class);
		
		job.setOutputFormatClass(ClientTransferOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		
		return job;
	}
	
	protected void afterJobFinish(int ret, String indexName) throws InterruptedException, ExecutionException {
		if (ret == 0) {
			//reopen after finished creating index's data
			OpenIndexRequest openIndexRequest = new OpenIndexRequest(indexName);
			OpenIndexResponse openResp = EsMdFactory.getTransportClient().admin().indices().open(openIndexRequest).get();
			if (!openResp.isAcknowledged()) {
				throw new IllegalStateException("re-open index [" + indexName + "] fail");
			}
		}
		else {
			DeleteIndexRequest deleteRequest = new DeleteIndexRequest(indexName);
			DeleteIndexResponse deleteResp = EsMdFactory.getTransportClient().admin().indices().delete(deleteRequest).get(); 
			if (!deleteResp.isAcknowledged()) {
				throw new IllegalStateException("delete index [" + indexName + "] fail");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new HesImportJob(), args);
		System.exit(res);
	}

}
