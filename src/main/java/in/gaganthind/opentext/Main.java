package in.gaganthind.opentext;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Main {

    public static void main(String[] args) {
        var i = new AtomicInteger();
        Callable<String> work = () -> {
            Thread.sleep(100);
            return Thread.currentThread().getName() + " result is : " + (i.incrementAndGet());
        };

        Callable<String> badTask = () -> {
            Thread.sleep(100);
            throw new RuntimeException(Thread.currentThread().getName() + " execution resulted in bad task");
        };

        List<Task<String>> tasks = new ArrayList<>();

        // Group One
        var taskGroupOne = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.WRITE, work));
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.WRITE, badTask));
        for (int j = 0; j < 4; j++) {
            tasks.add(new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.READ, work));
        }

        // Group Two
        var taskGroupTwo = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.WRITE, work));
        for (int j = 0; j < 4; j++) {
            tasks.add(new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.READ, work));
        }

        // Group Three
        var taskGroupThree = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.WRITE, work));
        for (int j = 0; j < 4; j++) {
            tasks.add(new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.READ, work));
        }

        // Execute the jobs
        TaskExecutorService taskExecutor = TaskExecutorService.newFixedThreadPool(6);

        tasks.stream()
                .map(taskExecutor::submitTask)
                .toList()
                .forEach(job -> {
                    try {
                        System.out.println(job.get());
                    } catch (InterruptedException e) {
                        System.out.printf("Job interrupted with exception: %s%n", e.getMessage());
                    } catch (ExecutionException e) {
                        System.out.printf("Job failed with exception: %s%n", e.getMessage());
                    }
                });

        taskExecutor.shutdown();
        taskExecutor.submitTask(new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.WRITE, work));
    }

    /**
     * TaskExecutor implementation class providing below-mentioned functionality.
     *     1. Tasks can be submitted concurrently. Task submission should not block the submitter.
     *     2. Tasks are executed asynchronously and concurrently. Maximum allowed concurrency may be restricted.
     *     3. Once task is finished, its results can be retrieved from the Future received during task submission.
     *     4. The order of tasks must be preserved.
     *         ◦ The first task submitted must be the first task started.
     *         ◦ The task result should be available as soon as possible after the task completes.
     *     5. Tasks sharing the same TaskGroup must not run concurrently.
     */
    private static class TaskExecutorService implements TaskExecutor {

        private final Set<Worker> workers;

        private final int maxThreadLimit;

        private final BlockingQueue<TaskWithFutureTask> workQueue;

        private int currentThreadCount;

        private final AtomicBoolean isShutdownRequested;

        /**
         * Map based on TaskGroup.groupUUID (as key) and a ReenterantReadWriteLock (as value).
         * For a given TaskGroup.groupUUID, only one Write thread and multiple read threads would be able to execute as only 1 mutex object is available.
         */
        private final Map<String, ReentrantReadWriteLock> mutexMap;

        private TaskExecutorService(int numberOfThreads) {
            maxThreadLimit = Math.min(numberOfThreads, Runtime.getRuntime().availableProcessors());
            workers = new HashSet<>(maxThreadLimit);
            currentThreadCount = 0;
            workQueue = new LinkedBlockingQueue<>();
            mutexMap = new ConcurrentHashMap<>();
            isShutdownRequested = new AtomicBoolean(false);
        }

        /**
         * Creating new TaskExecutorService instance using provided max thread count.
         *
         * @param numberOfThreads - Total number of maximum threads allowed in the thread-pool.
         * @return TaskExecutorService instance
         */
        public static TaskExecutorService newFixedThreadPool(int numberOfThreads) {
            return new TaskExecutorService(numberOfThreads);
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            if (isShutdownRequested.get()) {
                throw new IllegalStateException("ExecutorService shutdown requested, no more task submission possible");
            }

            if (task == null) {
                throw new NullPointerException("Provided task is null");
            }

            mutexMap.computeIfAbsent(task.taskGroup.groupUUID.toString(), k -> new ReentrantReadWriteLock());

            /*
             * Callable (provided in the task) will be run by the FutureTask, it will return a Future that the user can use to get the results later.
             * The workers will run the FutureTask as a runnable and result can be fetched later.
             */
            RunnableFuture<T> futureTask = new FutureTask<>(task.taskAction);
            workQueue.offer(new TaskWithFutureTask(task, futureTask));

            addWorkerIfNeeded();

            return futureTask;
        }

        /**
         * Add a worker only when the worker count is less than maxThreadLimit.
         * Otherwise, the existing workers will pull tasks from the workQueue.
         */
        private void addWorkerIfNeeded() {
            if (currentThreadCount >= maxThreadLimit) {
                return;
            }

            currentThreadCount++;
            Worker worker = new Worker(workQueue, mutexMap);
            workers.add(worker);
            worker.thread.start();
        }

        /**
         * Shutdown functionality for the executor service.
         */
        public void shutdown() {
            if (isShutdownRequested.get()) {
                System.err.println("ExecutorService already shutdown.");
                return;
            }

            this.isShutdownRequested.set(true);

            for (Worker worker : workers) {
                while (worker.isRunning) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException e) {
                    }
                }
                worker.thread.interrupt();
            }
        }
    }

    /**
     * Record representing the original task along with the futureTask that was submitted to executor service.
     *
     * @param task - Original task submitted to executor service
     * @param futureTask - RunnableFuture that's created internally by the executor service from the provided task.
     */
    private record TaskWithFutureTask(Task<?> task, RunnableFuture<?> futureTask) {
    }

    /**
     * Worker that would be running/executing the provided tasks.
     */
    private static class Worker implements Runnable {
        private final BlockingQueue<TaskWithFutureTask> blockingQueue;
        private final Thread thread;
        private final Map<String, ReentrantReadWriteLock> mutexMap;
        private volatile boolean isRunning;

        Worker(BlockingQueue<TaskWithFutureTask> blockingQueue, Map<String, ReentrantReadWriteLock> mutexMap) {
            this.blockingQueue = blockingQueue;
            this.thread = new Thread(this);
            this.mutexMap = mutexMap;
            this.isRunning = false;
        }

        @Override
        public void run() {
            while (true) {
                TaskWithFutureTask data;
                try {
                    data = blockingQueue.take();
                } catch (InterruptedException e) {
                    System.out.printf("Interrupt received for the thread %s%n", Thread.currentThread().getName());
                    break;
                }

                var lock = mutexMap.get(data.task.taskGroup.groupUUID.toString());
                if (TaskType.WRITE == data.task.taskType) {
                    try {
                        lock.writeLock().lock();
                        this.isRunning = true;
                        performTask(data);
                    } finally {
                        this.isRunning = false;
                        System.out.printf("Released Lock - %s Task %s belonging to group %s with thread %s - end time : %d%n",
                                data.task.taskType, data.task.taskUUID, data.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
                        lock.writeLock().unlock();
                    }
                } else {
                    try {
                        lock.readLock().lock();
                        this.isRunning = true;
                        performTask(data);
                    } finally {
                        this.isRunning = false;
                        System.out.printf("Released Lock - %s Task %s belonging to group %s with thread %s - end time : %d%n",
                                data.task.taskType, data.task.taskUUID, data.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
                        lock.readLock().unlock();
                    }
                }
            }
        }

        private static void performTask(TaskWithFutureTask data) {
            System.out.printf("Started - %s Task %s belonging to group %s with thread %s - start time : %d%n",
                    data.task.taskType, data.task.taskUUID, data.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
            try {
                data.futureTask.run();
            } catch (Throwable ex) { // Should work but doesn't
                var message = ex.getMessage() != null ? ex.getMessage() : "";
                System.out.printf("Exception - in %s Task %s belonging to group %s with thread %s - time : %d with message %s%n",
                        data.task.taskType, data.task.taskUUID, data.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis(), message);
                throw ex;
            } finally {
                System.out.printf("Finished - %s Task %s belonging to group %s with thread %s - end time : %d%n",
                        data.task.taskType, data.task.taskUUID, data.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
            }
        }
    }

    /**
     * Enumeration of task types.
     */
    public enum TaskType {
        READ,
        WRITE,
    }

    public interface TaskExecutor {
        /**
         * Submit new task to be queued and executed.
         *
         * @param task Task to be executed by the executor. Must not be null.
         * @return Future for the task asynchronous computation result.
         */
        <T> Future<T> submitTask(Task<T> task);
    }

    /**
     * Representation of computation to be performed by the {@link TaskExecutor}.
     *
     * @param taskUUID   Unique task identifier.
     * @param taskGroup  Task group.
     * @param taskType   Task type.
     * @param taskAction Callable representing task computation and returning the result.
     * @param <T>        Task computation result value type.
     */
    public record Task<T>(
            UUID taskUUID,
            TaskGroup taskGroup,
            TaskType taskType,
            Callable<T> taskAction
    ) {
        public Task {
            if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

    /**
     * Task group.
     *
     * @param groupUUID Unique group identifier.
     */
    public record TaskGroup(
            UUID groupUUID
    ) {
        public TaskGroup {
            if (groupUUID == null) {
                throw new IllegalArgumentException("All parameters must not be null");
            }
        }
    }

}