/**
 * 
 */
package com.hiido.eagle.hes.transfer;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.FutureCallback;

/**
 * @author dengqibin
 *
 */
public class FileTransferFuture<T> implements Future<T> {
	
	private volatile boolean isFinished = false;
	private volatile boolean isSuccessed = false;
	private T resp;
	private final FutureCallback<T> callback;
	private volatile boolean isDone = false;
	
	public FileTransferFuture() {
		this(null);
	}
	
	public FileTransferFuture(FutureCallback<T> callback) {
		this.callback = callback;
	}
	
	protected void onSuccess(T resp) {
		this.resp = resp;
		this.isFinished = true;
		this.isSuccessed = true;
		synchronized (this) {
			this.notifyAll();
		}
		if (this.callback != null) {
			this.callback.onSuccess(this.resp);
		}
	}
	
	protected void onFailure(Throwable t) {
		this.resp = null;
		this.isFinished = true;
		this.isSuccessed = false;
		synchronized (this) {
			this.notifyAll();
		}
		if (this.callback != null) {
			this.callback.onFailure(t);
		}
	}
	
	protected void setDone() {
		this.isDone = true;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return this.isDone;
	}
	
	public boolean isSuccessed() {
		return this.isSuccessed;
	}

	@Override
	public T get() throws InterruptedException {
		if (!isFinished) {
			synchronized (this) {
				this.wait();
			}
		}
		return this.resp;
	}

	@Override
	public T get(long timeout, TimeUnit unit)
			throws IllegalArgumentException, InterruptedException,
			TimeoutException {
		if (unit != TimeUnit.MILLISECONDS) {
			throw new IllegalArgumentException("Only suppport MILLISECONDS TimeUnit");
		}
		
		if (!isFinished) {
			synchronized (this) {
				this.wait(timeout);
			}
		}
		return this.resp;
	}
	
}
