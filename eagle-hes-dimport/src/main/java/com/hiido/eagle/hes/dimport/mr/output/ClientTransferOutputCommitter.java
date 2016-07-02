package com.hiido.eagle.hes.dimport.mr.output;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;
import com.hiido.eagle.hes.dimport.client.IndexTransferBootstrapFactory;
import com.hiido.eagle.hes.dimport.client.IndexTransferClient;
import com.hiido.eagle.hes.dimport.client.IndexTransferClient.IndexTransferInfo;
import com.hiido.eagle.hes.dimport.common.Conts;

public class ClientTransferOutputCommitter extends FileOutputCommitter {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClientTransferOutputCommitter.class);
	
	private final File localTmpPath;
	
	private final String indexName;
	private final String primaryHost;
	private final String[] replicaHosts;
	private final int shardId;
	private final boolean cleanLocalTmpPath;

	public ClientTransferOutputCommitter(Path hdfsSyncPath, TaskAttemptContext context) throws IOException {
		super(hdfsSyncPath, context);
		
		Configuration conf = context.getConfiguration();

		indexName = conf.get(Conts.PRO_INDEX_NAME);
		shardId = context.getTaskAttemptID().getTaskID().getId();
		primaryHost = conf.get("shard" + shardId + ".primary.host");
		
		String replicaHostStr = conf.get("shard" + shardId + ".replica.host");
		if (!Strings.isNullOrEmpty(replicaHostStr)) {
			replicaHosts = replicaHostStr.split(",");
		}
		else {
			replicaHosts = null;
		}
		
		String[] tmpPaths = conf.get(Conts.PRO_LOCAL_TMP_PATH).split(",");
		String tmpPath = tmpPaths[shardId % tmpPaths.length];
		
		localTmpPath = new File (tmpPath
				+ File.separator + "tmp"
				+ File.separator + context.getTaskAttemptID().toString());
		
		if (localTmpPath.exists()) {
			localTmpPath.delete();
		}
		
		if (!localTmpPath.mkdirs()) {
	        throw new IOException(String.format("can not create [%s] on local file system", localTmpPath.getPath()));
	    }
		
		cleanLocalTmpPath = conf.getBoolean(Conts.PRO_CLEAN_LOCAL_TMP_PATH, true);
	}

	public File getLocalTmpPath() {
		return localTmpPath;
	}

	@Override
	public void abortJob(JobContext context, State state) throws IOException {
		deleteLocalTmpPath();
		super.abortJob(context, state);
	}

	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
		deleteLocalTmpPath();
		super.abortTask(context);
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {
		if (localTmpPath.exists()) {
			try {
				transferToServer(context);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
		
		super.commitTask(context);
		deleteLocalTmpPath();
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
		return localTmpPath.exists() || super.needsTaskCommit(context);
	}

	private void transferToServer(TaskAttemptContext context) throws IOException, InterruptedException {
		
		IndexTransferBootstrapFactory clientBootstrapFactory = new IndexTransferBootstrapFactory();
		
		try {
			if (replicaHosts != null && replicaHosts.length > 0) {
				
				final CountDownLatch latch = new CountDownLatch(1 + replicaHosts.length);
				
				ExecutorService executorService = Executors.newFixedThreadPool(1 + replicaHosts.length);
				
				TransferTask primaryTask = new TransferTask(context, clientBootstrapFactory, new FutureCallback<Boolean>() {
					@Override
					public void onSuccess(Boolean result) {
						latch.countDown();
					}
					@Override
					public void onFailure(Throwable t) {
						latch.countDown();
					}
				}, localTmpPath, indexName, primaryHost, shardId, true);
				
				executorService.execute(primaryTask);
				
				TransferTask[] replicaTasks = new TransferTask[replicaHosts.length];
				for (int i = 0; i < replicaHosts.length; i++) {
					
					TransferTask replicaTask = new TransferTask(context, clientBootstrapFactory, new FutureCallback<Boolean>() {
						@Override
						public void onSuccess(Boolean result) {
							latch.countDown();
						}
						@Override
						public void onFailure(Throwable t) {
							latch.countDown();
						}
					}, localTmpPath, indexName, replicaHosts[i], shardId, false);
					
					replicaTasks[i] = replicaTask;
					
					executorService.execute(replicaTask);
				}
				
				latch.await();
				
				if (!primaryTask.isSuccessed()) {
					throw new IOException("transfer index [" + indexName +  "] primary shard [ " + shardId + " ] file err");
				}
				
				int successedReplicas = 0;
				for (int i = 0; i < replicaTasks.length; i++) {
					if (replicaTasks[i].isSuccessed()) {
						successedReplicas++;
					}
				}
				
				if (successedReplicas == 0) {
					throw new IOException("transfer index [" + indexName +  "] replica shard [ " + shardId + " ] file err");
				}
				else if (successedReplicas != replicaHosts.length) {
					LOG.warn("index [{}] primary shard [{}] has [{}] replica shards, but only [{}] replica shards transfer successfull", indexName, shardId, replicaHosts.length, successedReplicas);
				}
			}
			else {
				TransferTask primaryTask = new TransferTask(context, clientBootstrapFactory, new FutureCallback<Boolean>() {
					@Override
					public void onSuccess(Boolean result) {
					}
					@Override
					public void onFailure(Throwable t) {
					}
				}, localTmpPath, indexName, primaryHost, shardId, true);
				
				primaryTask.run();
				
				if (!primaryTask.isSuccessed()) {
					throw new IOException("transfer index [" + indexName +  "] primary shard [ " + shardId + " ] file err");
				}
			}
		} finally {
			clientBootstrapFactory.shutdown();
		}
	}
	
	private static class TransferTask implements Runnable {
		
		private final TaskAttemptContext context;
		private final IndexTransferBootstrapFactory clientBootstrapFactory;
		private final File localTmpPath;
		private final String indexName;
		private final String host;
		private final int shardId;
		private boolean primaryOrReplica;
		
		private boolean isSuccessed = false;
		
		private final FutureCallback<Boolean> callback;
		
		private TransferTask(TaskAttemptContext context, IndexTransferBootstrapFactory clientBootstrapFactory
				, FutureCallback<Boolean> callback
				, File localTmpPath, String indexName, String host, int shardId, boolean primaryOrReplica) {
			this.context = context;
			this.clientBootstrapFactory = clientBootstrapFactory;
			this.callback = callback;
			this.localTmpPath = localTmpPath;
			this.indexName = indexName;
			this.host = host;
			this.shardId = shardId;
			this.primaryOrReplica = primaryOrReplica;
		}

		@Override
		public void run() {
			try {
				for (File localFolder : localTmpPath.listFiles()) {
					if (validateFolder(localFolder)) {
						byte fileType = getFolderType(localFolder);
						for (File localFile : localFolder.listFiles()) {
							context.progress();
							transferFile(fileType, localFile);
						}
					}
				}
				isSuccessed = true;
				callback.onSuccess(true);
			} catch (Exception e) {
				isSuccessed = false;
				callback.onFailure(e);
			}
		}
		
		private void transferFile(byte fileType, File localFile) throws IOException {
			IndexTransferClient client = null;
			IndexTransferInfo info = null;
			try {
				client = IndexTransferClient.build(indexName, host, shardId, clientBootstrapFactory);
				info = new IndexTransferInfo(indexName, shardId, fileType, localFile.getPath());
				client.transfer(info);
			} catch (Exception e) {
				LOG.error("transfer index [{}] shard [{}]->[primaryOrReplica:{}] file [{}] err", indexName, shardId, primaryOrReplica, localFile.getPath(), e);
				throw new IOException("client transfer file " + localFile.getPath() + " err", e);
			} finally {
				Closeables.close(client, true);
			}
		}
		
		private boolean validateFolder(File localFolder) {
			if (localFolder.isDirectory() 
					&& (Conts.FILE_TYPE_INDEX_FOLDER.equals(localFolder.getName())
						|| Conts.FILE_TYPE_TRANSLOG_FOLDER.equals(localFolder.getName()))
				) {
				return true;
			}
			return false;
		}
		
		private byte getFolderType(File localFolder) {
			return Conts.FILE_TYPE_INDEX_FOLDER.equals(localFolder.getName()) ? Conts.FILE_TYPE_INDEX : Conts.FILE_TYPE_TRANSLOG;
		}

		private boolean isSuccessed() {
			return isSuccessed;
		}
	}

	private void deleteLocalTmpPath() throws IOException {
		if (cleanLocalTmpPath) {
			//FileUtils.deleteDirectory(localTmpPath);
			FileUtils.deleteQuietly(localTmpPath);
		}
	}
}
