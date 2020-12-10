package com.coreoz.wisp;

import com.coreoz.wisp.schedule.Schedule;

/**
 * A {@code Job} is the association of a {@link Runnable} process and its
 * running {@link Schedule}.<br/>
 * <br/>
 * A {@code Job} also contains information about its status and its running
 * statistics.
 */
public class Job {

	private final Integer id;
	private final String name;
	private final Runnable runnable;
	private volatile Runnable wrappedRunnable;
	private volatile Schedule schedule;
	private final boolean removeWhenDone;
	private volatile JobStatus status;
	private volatile long nextExecutionTimeInMillis;
	private volatile int executionsCount;
	private volatile Long lastExecutionStartedTimeInMillis;
	private volatile Long lastExecutionEndedTimeInMillis;
	private volatile Thread threadRunningJob;
	private volatile boolean cancelRequested;
	
	Job(Integer id, String name, Runnable runnable, Schedule schedule) {
		this(id, name, runnable, schedule, false);
	}
	
	Job(Integer id, String name, Runnable runnable, Schedule schedule, boolean removeWhenDone) {
		this.id = id;
		this.name = name;
		this.runnable = runnable;
		this.schedule = schedule;
		this.removeWhenDone = removeWhenDone;
	}
	
	public Integer id() {
		return id;
	}

	public String name() {
		return name;
	}

	public Runnable runnable() {
		return runnable;
	}
	
	Runnable wrappedRunnable() {
		return wrappedRunnable;
	}
	
	void wrappedRunnable(Runnable wrappedRunnable) {
		this.wrappedRunnable = wrappedRunnable;
	}

	public Schedule schedule() {
		return schedule;
	}

	void schedule(Schedule schedule) {
		this.schedule = schedule;
	}
	
	public boolean removeWhenDone() {
		return removeWhenDone;
	}

	public JobStatus status() {
		return status;
	}

	void status(JobStatus status) {
		this.status = status;
	}

	public long nextExecutionTimeInMillis() {
		return nextExecutionTimeInMillis;
	}

	void nextExecutionTimeInMillis(long nextExecutionTimeInMillis) {
		this.nextExecutionTimeInMillis = nextExecutionTimeInMillis;
	}

	public int executionsCount() {
		return executionsCount;
	}

	void executionsCount(int executionsCount) {
		this.executionsCount = executionsCount;
	}

	public Long lastExecutionStartedTimeInMillis() {
		return lastExecutionStartedTimeInMillis;
	}

	void lastExecutionStartedTimeInMillis(Long lastExecutionStartedTimeInMillis) {
		this.lastExecutionStartedTimeInMillis = lastExecutionStartedTimeInMillis;
	}

	public Long lastExecutionEndedTimeInMillis() {
		return lastExecutionEndedTimeInMillis;
	}

	void lastExecutionEndedTimeInMillis(Long lastExecutionEndedTimeInMillis) {
		this.lastExecutionEndedTimeInMillis = lastExecutionEndedTimeInMillis;
	}
	
	Thread threadRunningJob() {
		return threadRunningJob;
	}
	
	void threadRunningJob(Thread threadRunningJob) {
		this.threadRunningJob = threadRunningJob;
	}
	
	boolean cancelRequested() {
		return cancelRequested;
	}
	
	void cancelRequested(boolean cancelRequested) {
		this.cancelRequested = cancelRequested;
	}
	
	@Override
	public String toString() {
		return String.format(
				"Job [id=%s, name=%s, schedule=%s, status=%s, nextExecutionTimeInMillis=%s, executionsCount=%s, lastExecutionStartedTimeInMillis=%s, lastExecutionEndedTimeInMillis=%s]",
				id, name, schedule, status, nextExecutionTimeInMillis, executionsCount,
				lastExecutionStartedTimeInMillis, lastExecutionEndedTimeInMillis);
	}
}
