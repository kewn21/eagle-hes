package com.hiido.eagle.hes.dimport.mr.output;

import java.io.IOException;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClientTransferOutputFormat<K, V> extends FileOutputFormat<K, V> {

	private ClientTransferOutputCommitter committer;

	@Override
	public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {

		if (committer == null) {
			committer = new ClientTransferOutputCommitter(super.getOutputPath(context), context);
		}

		return committer;
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		return new RecordWriter<K, V>() {
			@Override
			public void close(TaskAttemptContext context) throws IOException, InterruptedException { }

			@Override
			public void write(K key, V val) throws IOException, InterruptedException { }
		};
	}
}
