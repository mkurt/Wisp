package com.coreoz.wisp;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coreoz.wisp.schedule.Schedule;
import com.coreoz.wisp.stats.SchedulerStats;
import com.coreoz.wisp.stats.ThreadPoolStats;
import com.coreoz.wisp.time.TimeProvider;

import lombok.SneakyThrows;

/**
 * A {@code Scheduler} instance reference a group of jobs
 * and is responsible to schedule these jobs at the expected time.<br/>
 * <br/>
 * A job is executed only once at a time.
 * The scheduler will never execute the same job twice at a time.
 */
public final class Scheduler {

	private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
	
	private static final AtomicInteger jobIdGenerator = new AtomicInteger(0);
	
	private final ThreadPoolExecutor threadPoolExecutor;
	private final TimeProvider timeProvider;
	private final AtomicBoolean launcherNotifier;
	
	private final Map<Integer, Job> jobs;
	private final ArrayList<Job> nextExecutionsOrder;
	private final Map<Integer, CompletableFuture<Job>> cancelHandles;
	
	private volatile boolean shuttingDown;

	/**
	 * Create a scheduler with the defaults defined at {@link SchedulerConfig}
	 */
	public Scheduler() {
		this(SchedulerConfig.builder().build());
	}

	/**
	 * Create a scheduler with the defaults defined at {@link SchedulerConfig}
	 * and with a max number of worker threads
	 * @param maxThreads The maximum number of worker threads that can be created for the scheduler.
	 * @throws IllegalArgumentException if {@code maxThreads <= 0}
	 */
	public Scheduler(int maxThreads) {
		this(SchedulerConfig.builder().maxThreads(maxThreads).build());
	}

	/**
	 * Create a scheduler according to the configuration
	 * @throws IllegalArgumentException if one of the following holds:<br>
     * {@code SchedulerConfig#getMinThreads() < 0}<br>
     * {@code SchedulerConfig#getThreadsKeepAliveTime() < 0}<br>
     * {@code SchedulerConfig#getMaxThreads() <= 0}<br>
     * {@code SchedulerConfig#getMaxThreads() < SchedulerConfig#getMinThreads()}
     * @throws NullPointerException if {@code SchedulerConfig#getTimeProvider()} is {@code null}
	 */
	public Scheduler(SchedulerConfig config) {
		this.threadPoolExecutor = new ScalingThreadPoolExecutor(config.getMinThreads(),
			config.getMaxThreads(), config.getThreadsKeepAliveTime().toMillis(), TimeUnit.MILLISECONDS,
			new WispThreadFactory());
		this.timeProvider = Objects.requireNonNull(config.getTimeProvider(), "Time provider cannot be null");
		this.launcherNotifier = new AtomicBoolean(true);
		this.jobs = new ConcurrentHashMap<>();
		this.nextExecutionsOrder = new ArrayList<>();
		this.cancelHandles = new ConcurrentHashMap<>();
		startLauncherThread();
	}
	
	private void startLauncherThread() {
		Thread launcherThread = new Thread(this::launcher, "Wisp Monitor");
		if (launcherThread.isDaemon()) {
			launcherThread.setDaemon(false);
		}
		launcherThread.start();
	}

	/**
	 * The daemon that will be in charge of placing the jobs in the thread pool
	 * when they are ready to be executed.
	 */
	@SneakyThrows
	private void launcher() {
		while (!shuttingDown) {
			Long timeBeforeNextExecution = null;
			synchronized (this) {
				if (nextExecutionsOrder.size() > 0) {
					timeBeforeNextExecution = nextExecutionsOrder.get(0).nextExecutionTimeInMillis() 
							- timeProvider.currentTime();
				}
			}
			if (timeBeforeNextExecution == null || timeBeforeNextExecution > 0L) {
				synchronized (launcherNotifier) {
					if (shuttingDown) {
						return;
					}
					// If someone has notified the launcher
					// then the launcher must check again the next job to execute.
					// We must be sure not to miss any changes that would have
					// happened after the timeBeforeNextExecution calculation.
					if (launcherNotifier.get()) {
						if (timeBeforeNextExecution == null) {
							launcherNotifier.wait();
						} else {
							launcherNotifier.wait(timeBeforeNextExecution);
						}
					}
					launcherNotifier.set(true);
				}
			} else {
				synchronized (this) {
					if (shuttingDown) {
						return;
					}
					if (nextExecutionsOrder.size() > 0) {
						Job jobToRun = nextExecutionsOrder.remove(0);
						jobToRun.status(JobStatus.READY);
						if (jobToRun.wrappedRunnable() == null) {
							jobToRun.wrappedRunnable(() -> runJob(jobToRun));
						}
						threadPoolExecutor.execute(jobToRun.wrappedRunnable());
						if (threadPoolExecutor.getActiveCount() == threadPoolExecutor.getMaximumPoolSize()) {
							logger.warn(
								"Job thread pool is full, either tasks take too much time to execute"
								+ " or either the thread pool is too small"
							);
						}
					}
				}
			}
		}
	}

	/**
	 * The wrapper around a job that will be executed in the thread pool.
	 * It is especially in charge of logging, changing the job status
	 * and checking for the next job to be executed.
	 * @param jobToRun the job to execute
	 */
 	private void runJob(Job jobToRun) {
		long startExecutionTime = timeProvider.currentTime();
		long timeBeforeNextExecution = jobToRun.nextExecutionTimeInMillis() - startExecutionTime;
		if (timeBeforeNextExecution < 0) {
			logger.debug("{} execution is {} ms late", jobToRun, -timeBeforeNextExecution);
		}
		
		jobToRun.status(JobStatus.RUNNING);
		jobToRun.lastExecutionStartedTimeInMillis(startExecutionTime);
		jobToRun.threadRunningJob(Thread.currentThread());

		try {
			jobToRun.runnable().run();
		} catch(Throwable t) {
			logger.error("Error during {} execution", jobToRun, t);
		}
		
		jobToRun.executionsCount(jobToRun.executionsCount() + 1);
		jobToRun.lastExecutionEndedTimeInMillis(timeProvider.currentTime());
		jobToRun.threadRunningJob(null);

		if (logger.isDebugEnabled()) {
			logger.debug("{} executed in {} ms", jobToRun, timeProvider.currentTime() - startExecutionTime);
		}
	
		scheduleNextExecution(jobToRun);
	}
	
	/**
	 * Schedule the executions of a process.
	 *
	 * @param runnable The process to be executed at a schedule
	 * @param when The {@link Schedule} at which the process will be executed
	 * @return The corresponding {@link Job} created.
	 * @throws NullPointerException if {@code runnable} or {@code when} are {@code null}
	 */
	public Job schedule(Runnable runnable, Schedule when) {
		return schedule(null, runnable, when);
	}

	/**
	 * Schedule the executions of a process.<br>
	 * 
	 * @param name The name of the created job
	 * @param runnable The process to be executed at a schedule
	 * @param when The {@link Schedule} at which the process will be executed
	 * @return The corresponding {@link Job} created.
	 * @throws NullPointerException if {@code runnable} or {@code when} are {@code null}
	 */
	public Job schedule(String name, Runnable runnable, Schedule when) {
		return schedule(name, runnable, when, false);
	}
	
	/**
	 * Schedule the executions of a process.<br>
	 * 
	 * @param name The name of the created job
	 * @param runnable The process to be executed at a schedule
	 * @param when The {@link Schedule} at which the process will be executed
	 * @param removeWhenDone flag that determines whether job should be kept or not when status is DONE
	 * @return The corresponding {@link Job} created.
	 * @throws NullPointerException if {@code runnable} or {@code when} are {@code null}
	 */
	public Job schedule(String name, Runnable runnable, Schedule when, boolean removeWhenDone) {
		Objects.requireNonNull(runnable, "Runnable must not be null");
		Objects.requireNonNull(when, "Schedule must not be null");
		String jobName = name == null || name.isEmpty() ? runnable.toString() : name;
		Job job = new Job(jobIdGenerator.incrementAndGet(), jobName, runnable, when, removeWhenDone);
		logger.info("Scheduling job '{}' to run {}", job.name(), job.schedule());
		scheduleNextExecution(job);
		jobs.put(job.id(), job);
		return job;
	}
	
	/**
	 * Reschedules all current jobs, use when a time shift occurs
	 */
	public void rescheduleAll() {
		synchronized (this) {
			for (Job job : jobs.values()) {
				Job duplicateJob = new Job(job.id(), job.name(), job.runnable(), job.schedule(), job.removeWhenDone());
				cancelImpl(job).thenAccept(cancelledJob -> {
					synchronized (this) {
						if (cancelledJob.cancelRequested()) {
							logger.info("Rescheduling job {} skipped because of the cancel request", duplicateJob);
							return;
						}
						logger.info("Rescheduling job {} to run {}", duplicateJob, duplicateJob.schedule());
						duplicateJob.executionsCount(cancelledJob.executionsCount());
						duplicateJob.lastExecutionStartedTimeInMillis(cancelledJob.lastExecutionStartedTimeInMillis());
						duplicateJob.lastExecutionEndedTimeInMillis(cancelledJob.lastExecutionEndedTimeInMillis());
						scheduleNextExecution(duplicateJob);
						jobs.put(duplicateJob.id(), duplicateJob);
					}
				});
			}
		}
	}
		
	private void scheduleNextExecution(Job job) {
		synchronized (this) {
			long currentTimeInMillis = timeProvider.currentTime();
			try {
				job.nextExecutionTimeInMillis(
					job.schedule().nextExecutionInMillis(
						currentTimeInMillis, job.executionsCount(), job.lastExecutionEndedTimeInMillis()
					)
				);
			} catch (Throwable t) {
				job.nextExecutionTimeInMillis(Schedule.WILL_NOT_BE_EXECUTED_AGAIN);
				logger.error(
					"An exception was raised during the job next execution time calculation,"
					+ " therefore {} will not be executed again.", job, t
				);
			}
			if (job.nextExecutionTimeInMillis() >= currentTimeInMillis) {
				job.status(JobStatus.SCHEDULED);
				nextExecutionsOrder.add(job);
				nextExecutionsOrder.sort(Comparator.comparing(
					Job::nextExecutionTimeInMillis
				)); 
				synchronized (launcherNotifier) {
					launcherNotifier.set(false);
					launcherNotifier.notify();
				}
			} else {
				logger.info(
					"{} will not be executed again since its next execution time, {} ms, is planned in the past",
					job, Instant.ofEpochMilli(job.nextExecutionTimeInMillis())
				);
				job.status(JobStatus.DONE);
				if (job.removeWhenDone()) {
					jobs.remove(job.id());
				}
				CompletableFuture<Job> cancelHandle = cancelHandles.remove(job.id());
				if (cancelHandle != null) {
					jobs.remove(job.id());
					cancelHandle.complete(job);
				}
			}
		}
	}
	
	/**
	 * Fetch the status of all the jobs that has been registered on the {@code Scheduler}
	 * including the {@link JobStatus#DONE} jobs
	 */
	public Collection<Job> jobStatus() {
		return jobs.values();
	}

	/**
	 * Find a job by its id
	 */
	public Optional<Job> findJob(Integer jobId) {
		return Optional.ofNullable(jobs.get(jobId));
	}

	/**
	 * Issue a cancellation order for a job and
	 * returns immediately a promise that enables to follow the job cancellation status<br>
	 * <br>
	 * If the job is running, the scheduler will wait until it is finished to remove it
	 * from the jobs pool.
	 * If the job is not running, the job will just be removed from the pool.<br>
	 * After the job is cancelled, the job has the status {@link JobStatus#DONE}.
	 *
	 * @param jobId The job id to cancel
	 * @return The promise that succeed when the job is correctly cancelled
	 * and will not be executed again. If the job is running when {@link #cancel(Integer)}
	 * is called, the promise will succeed when the job has finished executing.
	 * if there is no job corresponding to the job id empty optional is returned.
	 */
	public CompletableFuture<Optional<Job>> cancel(Integer jobId) {
		synchronized (this) {
			Job job = findJob(jobId).orElse(null);
			if (job == null) {
				CompletableFuture<Optional<Job>> future = new CompletableFuture<>();
				future.complete(Optional.empty());
				return future;
			} else {
				job.cancelRequested(true);
				return cancelImpl(job).thenApply(cancelledJob -> Optional.of(cancelledJob));
			}
		}
	}
	
	private CompletableFuture<Job> cancelImpl(Job job) {
		synchronized (this) {
			JobStatus jobStatus = job.status();
			if (jobStatus == JobStatus.DONE) {
				jobs.remove(job.id());
				return CompletableFuture.completedFuture(job);
			}
			CompletableFuture<Job> existingHandle = cancelHandles.get(job.id());
			if (existingHandle != null) {
				return existingHandle;
			}
			job.schedule(Schedule.willNeverBeExecuted);
			if (jobStatus == JobStatus.READY && threadPoolExecutor.remove(job.wrappedRunnable())) {
				scheduleNextExecution(job);
				jobs.remove(job.id());
				return CompletableFuture.completedFuture(job);
			}
			// if the job status is/was READY but could not be removed from the thread pool,
			// then we have to wait for it to finish
			if (jobStatus == JobStatus.RUNNING || jobStatus == JobStatus.READY) {
				CompletableFuture<Job> promise = new CompletableFuture<>();
				cancelHandles.put(job.id(), promise);
				return promise;
			} else {
				for (Iterator<Job> iterator = nextExecutionsOrder.iterator(); iterator.hasNext();) {
					Job nextJob = iterator.next();
					if (nextJob == job) {
						iterator.remove();
						job.status(JobStatus.DONE);
						jobs.remove(job.id());
						return CompletableFuture.completedFuture(job);
					}
				}
				throw new IllegalStateException("Cannot find the job in nextExecutionsOrder");
			}
		}
	}

	/**
	 * Wait until the current running jobs are executed
	 * and cancel jobs that are planned to be executed.
	 * There is a 10 seconds timeout
	 */
	public void shutdown() {
		shutdown(Duration.ofSeconds(10));
	}

	/**
	 * Wait until the current running jobs are executed
	 * and cancel jobs that are planned to be executed.
	 * @param timeout The maximum time to wait
	 */
	public void shutdown(Duration timeout) {
		if (!shuttingDown) {
			synchronized (this) {
				if (shuttingDown) {
					return;
				}
				logger.info("Shutting down scheduler...");
				shuttingDown = true;
				threadPoolExecutor.shutdown(); 
				// stops jobs that have not yet started to be executed
				for (Job job : jobStatus()) {
					cancel(job.id());
				}
			}
			synchronized (launcherNotifier) {
				launcherNotifier.set(false);
				launcherNotifier.notify();
			}
			try {
				// Wait a while for existing tasks to terminate
			    if (!threadPoolExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
			       threadPoolExecutor.shutdownNow(); // Cancel currently executing tasks
			       // Wait a while for tasks to respond to being cancelled
			       if (!threadPoolExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
			           logger.error("Pool did not terminate");
			       }
			    }
			} catch (InterruptedException e) {
				 // (Re-)Cancel if current thread also interrupted
			     threadPoolExecutor.shutdownNow();
			     // Preserve interrupt status
			     Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Fetch statistics about the current {@code Scheduler}
	 */
	public SchedulerStats stats() {
		int activeThreads = threadPoolExecutor.getActiveCount();
		return SchedulerStats.of(ThreadPoolStats.of(
			threadPoolExecutor.getCorePoolSize(),
			threadPoolExecutor.getMaximumPoolSize(),
			activeThreads,
			threadPoolExecutor.getPoolSize() - activeThreads,
			threadPoolExecutor.getLargestPoolSize()
		));
	}

	private static class WispThreadFactory implements ThreadFactory {
		
		private static final AtomicInteger threadCounter = new AtomicInteger(0);
		
		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(r, "Wisp Scheduler Worker #" + threadCounter.getAndIncrement());
			if (thread.isDaemon()) {
				thread.setDaemon(false);
			}
			if (thread.getPriority() != Thread.NORM_PRIORITY) {
				thread.setPriority(Thread.NORM_PRIORITY);
			}
			return thread; 
		}
	}
}
