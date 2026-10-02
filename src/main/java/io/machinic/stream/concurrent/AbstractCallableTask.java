/*
 * Copyright 2026 Machinic.io LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.machinic.stream.concurrent;

import io.machinic.stream.StreamInterruptedException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Abstract base class for tasks that can be executed asynchronously and have a result or exception.
 */
public abstract class AbstractCallableTask<OUT> implements RunnableFuture<OUT> {
	
	private static final VarHandle STATE;
	private static final VarHandle RUNNER;
	
	static {
		try {
			MethodHandles.Lookup l = MethodHandles.lookup();
			STATE = l.findVarHandle(AbstractCallableTask.class, "state", int.class);
			RUNNER = l.findVarHandle(AbstractCallableTask.class, "runner", Thread.class);
		} catch (ReflectiveOperationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}
	
	private final CountDownLatch taskLatch = new CountDownLatch(1);
	private volatile Thread runner;
	private volatile int state = TaskStatus.PENDING;
	
	private final long scheduleNanoTime = System.nanoTime();
	private volatile long startNanoTime;
	private volatile long endNanoTime;
	
	private volatile Exception exception;
	private volatile OUT result;
	
	protected boolean compareAndSetRunner(Thread currentRunner, Thread runner) {
		return RUNNER.compareAndSet(this, null, Thread.currentThread());
	}
	
	protected void setRunner(Thread runner) {
		RUNNER.setVolatile(this, Thread.currentThread());
	}
	
	protected boolean compareAndSetState(int currentState, int state) {
		return STATE.compareAndSet(this, currentState, state);
	}
	
	protected void setState(int state) {
		STATE.setVolatile(this, state);
	}
	
	protected int getState() {
		return (int) STATE.getVolatile(this);
	}
	
	protected abstract OUT callTask();
	
	@Override
	public void run() {
		// Clear interrupted status
		//noinspection ResultOfMethodCallIgnored
		Thread.interrupted();
		
		setRunner(Thread.currentThread());
		
		try {
			if (!compareAndSetState(TaskStatus.PENDING, TaskStatus.RUNNING)) {
				return; // If task is no longer pending exit
			}
			startNanoTime = System.nanoTime();
			
			try {
				// run task
				result = callTask();
				
				// If we finished task mark as done even if it was canceled
				setState(TaskStatus.DONE);
			} catch (Exception e) {
				this.exception = e;
				if (e instanceof StreamInterruptedException) {
					setState(TaskStatus.CANCELED);
				} else {
					setState(TaskStatus.FAILED);
				}
			} finally {
				endNanoTime = System.nanoTime();
			}
		} finally {
			RUNNER.compareAndSet(this, Thread.currentThread(), null);
			taskLatch.countDown();
			// Clear interrupted status
			//noinspection ResultOfMethodCallIgnored
			Thread.interrupted();
		}
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (this.compareAndSetState(TaskStatus.PENDING, TaskStatus.CANCELED)) {
			return true;
		}
		
		if (mayInterruptIfRunning) {
			Thread runner = this.runner;
			if (runner != null) {
				runner.interrupt();
				setState(TaskStatus.CANCELED);
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public boolean isCancelled() {
		return TaskStatus.CANCELED == this.getState();
	}
	
	@Override
	public boolean isDone() {
		return taskLatch.getCount() <= 0;
	}
	
	public void await() throws InterruptedException {
		this.taskLatch.await();
	}
	
	public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
		return this.taskLatch.await(timeout, unit);
	}
	
	public Exception getException() {
		return this.exception;
	}
	
	public long getPendingDuration() {
		if (this.scheduleNanoTime != 0 && this.startNanoTime != 0) {
			return this.scheduleNanoTime - this.startNanoTime;
		}
		return 0L;
	}
	
	public long getDuration() {
		if (this.startNanoTime != 0 && this.endNanoTime != 0) {
			return this.endNanoTime - this.startNanoTime;
		}
		return 0L;
	}
	
	public OUT getResult() {
		return result;
	}
	
	@Override
	public OUT get() throws InterruptedException, ExecutionException {
		this.taskLatch.await();
		if (exception != null) {
			if (exception instanceof InterruptedException) {
				throw (InterruptedException) exception;
			} else {
				throw new ExecutionException(exception);
			}
		}
		return this.result;
	}
	
	@Override
	public OUT get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (!this.taskLatch.await(timeout, timeUnit)) {
			throw new TimeoutException("Task timed out after " + timeout + " " + timeUnit);
		}
		if (exception != null) {
			if (exception instanceof InterruptedException) {
				throw (InterruptedException) exception;
			} else {
				throw new ExecutionException(exception);
			}
		}
		return this.result;
	}
	
}
