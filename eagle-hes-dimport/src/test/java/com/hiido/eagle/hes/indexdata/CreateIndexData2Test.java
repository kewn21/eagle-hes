package com.hiido.eagle.hes.indexdata;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.routing.HashFunction;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.lucene.all.AllEntries;
import org.elasticsearch.common.lucene.all.AllField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MappedFieldType.Names;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper.CustomIntegerNumericField;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper.IntegerFieldType;
import org.elasticsearch.index.mapper.core.LongFieldMapper.CustomLongNumericField;
import org.elasticsearch.index.mapper.core.LongFieldMapper.LongFieldType;
import org.elasticsearch.index.mapper.core.NumberFieldMapper.NumberFieldType;
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

public class CreateIndexData2Test {
	
	protected final Logger LOG = LoggerFactory.getLogger(CreateIndexData2Test.class);
		
	private static final String ROOT_PATH = "/opt/work/data/indexdata";
	private static final String TRANSLOG_CODEC = "translog";
	private static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
	private static final int VERSION = VERSION_CHECKPOINTS;
	private static final int BUFFER_SIZE = RamUsageEstimator.NUM_BYTES_INT  // ops
            + RamUsageEstimator.NUM_BYTES_LONG // offset
            + RamUsageEstimator.NUM_BYTES_LONG;// generation
    
	@Test
	public void test01() throws IOException {
		
		String indexName = "idx_test_transfer8";
		String indexType = "idx_type";
		String shardId = "0";
		
		String rootPath = ROOT_PATH + File.separator + indexName + File.separator + shardId;
		
		Analyzer analyzer = new StandardAnalyzer();
	    IndexWriterConfig config = new IndexWriterConfig(analyzer);
	    ((TieredMergePolicy)config.getMergePolicy()).setFloorSegmentMB(50);
	    String indexPath = rootPath + "/index";
	    IndexWriter indexWriter = new IndexWriter(FSDirectory.open(Paths.get(indexPath)), config);
	    
		int num = 10;
		for (int i = 1; i <= num; i++) {
			XContentBuilder xbuilder = XContentFactory.jsonBuilder();
			xbuilder.startObject();
			xbuilder.field("col_1", (Object)"1");
			xbuilder.field("col_2", (Object)2);
			xbuilder.field("col_4", (Object)4L);
			xbuilder.endObject();
			
		    String uid = String.valueOf(i); //"c284131a-3c60-4744-9cb6-947107a0fd35";//UUID.randomUUID().toString();
		    System.out.println(uid);
			Document doc = new Document();
			
			BytesReference source = xbuilder.bytes();
			doc.add(new StoredField(SourceFieldMapper.Defaults.FIELD_TYPE.names().indexName(), source.array(), source.arrayOffset(), source.length()));
			indexWriter.addDocument(doc);
			
			doc.add(new Field(TypeFieldMapper.Defaults.FIELD_TYPE.names().indexName(), indexType, TypeFieldMapper.Defaults.FIELD_TYPE));
	        if (TypeFieldMapper.Defaults.FIELD_TYPE.hasDocValues()) {
	        	doc.add(new SortedSetDocValuesField(TypeFieldMapper.Defaults.FIELD_TYPE.names().indexName(), new BytesRef(indexType)));
	        }
	        
	        doc.add(new Field(UidFieldMapper.Defaults.FIELD_TYPE.names().indexName(), Uid.createUid(new StringBuilder(), indexType, uid), UidFieldMapper.Defaults.FIELD_TYPE));
	        
	        doc.add(new NumericDocValuesField(VersionFieldMapper.Defaults.FIELD_TYPE.names().indexName(), 1L));

	        
	        List<Field> fields = Lists.newArrayList();
	        
	        {
	        	String val = "1";
				MappedFieldType fieldType = new StringFieldType();
				fieldType.setHasDocValues(true);
				fieldType.setNames(new Names("col_1"));
				
				if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
					Field field = new Field("1", val, fieldType);
					//field.setBoost(DEFAULT_BOOST);
					fields.add(field);
				}
				if (fieldType.hasDocValues()) {
		            fields.add(new SortedSetDocValuesField(fieldType.names().indexName(), new BytesRef(val)));
		        }
	        }
	        
	        {
	        	int val = 2;
		        String colName = "col_2";
		        IntegerFieldType fieldType = new IntegerFieldType();
		        fieldType.setHasDocValues(true);
		        fieldType.setNames(new Names(colName));
		        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
					Field field = new CustomIntegerNumericField(val, fieldType);
					//field.setBoost(DEFAULT_BOOST);
					fields.add(field);
				}
				if (fieldType.hasDocValues()) {
					addDocValue(fieldType, fields, val);
				}
	        }
	        
	        {
	        	long val = 4L;
				NumberFieldType fieldType = new LongFieldType();
				fieldType.setHasDocValues(true);
				fieldType.setNames(new Names("col_4"));
				
				if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
					Field field = new CustomLongNumericField(val, fieldType);
					//field.setBoost(DEFAULT_BOOST);
					fields.add(field);
				}
				if (fieldType.hasDocValues()) {
					addDocValue(fieldType, fields, val);
				}
	        }
			
			for (Field field : fields) {
				doc.add(field);
			}
	        
	        AllEntries allEntries = new AllEntries();
	        allEntries.addText("col_1", "1", 0.1F);
	        allEntries.addText("col_2", "2", 0.1F);
	        allEntries.addText("col_4", "4", 0.1F);
	        Analyzer allEntriesAnalyzer = new FieldNameAnalyzer(analyzer);
	        
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
        
        String translogUUID = Strings.randomBase64UUID();
        Map<String, String> commitData = new HashMap<>(2);
        commitData.put(Translog.TRANSLOG_GENERATION_KEY, Long.toString(1L));
        commitData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
        indexWriter.setCommitData(commitData);
        indexWriter.close();
        
        final BytesRef ref = new BytesRef(translogUUID);
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC) + ref.length + RamUsageEstimator.NUM_BYTES_INT;
       
        String translogfilePath = rootPath + "/translog/";
        String logPath = translogfilePath + "translog-1.tlog";
        String ckpPath = translogfilePath + "translog.ckp";
        
        new File(translogfilePath).mkdir();
        new File(logPath).createNewFile();
        new File(ckpPath).createNewFile();
        
        Path logFilePath = Paths.get(logPath);
        Path ckpFilePath = Paths.get(ckpPath);
        
        try {
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
	            ckpOut.writeLong(1L);
	            Channels.writeToChannel(buffer, ckpChannel);
        	}
        } catch (Throwable throwable){
            LOG.error("translog file and ckp err", throwable);
        }
	}
	
	protected final void addDocValue(MappedFieldType fieldType, List<Field> fields, long value) {
		fields.add(new SortedNumericDocValuesField(fieldType.names().indexName(), value));
    }
	
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
	
	@Test
	public void test02() {
		final HashFunction hashFunction = new Murmur3HashFunction();
		String uid = "123";//org.elasticsearch.common.Strings.randomBase64UUID();
		int hash = hashFunction.hash(uid);
		System.out.println(hash);
		System.out.println(uid.hashCode());
		System.out.println(Math.abs(hash % 10));
		System.out.println(Math.abs(uid.hashCode()) % 10);
	}

}
