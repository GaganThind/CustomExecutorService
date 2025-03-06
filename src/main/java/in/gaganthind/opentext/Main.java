package in.gaganthind.opentext;

import java.util.*;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) {
        // Group One
        var taskGroupOne = new TaskGroup(UUID.randomUUID());

        Callable<String> readFive = () -> {
//            Thread.sleep(7000);
            return Thread.currentThread().getName() + " : " + 5;
        };
        Callable<String> readSix = () -> {
//            Thread.sleep(8000);
            return Thread.currentThread().getName() + " : " + 6;
        };

        var taskOne = new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.WRITE, readFive);
        var taskTwo = new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.READ, readSix);

        // Group Two
        var taskGroupTwo = new TaskGroup(UUID.randomUUID());
        Callable<String> readFour = () -> {
//            Thread.sleep(2000);
            return Thread.currentThread().getName() + " : " + 4;
        };
        Callable<String> readSeven = () -> {
//            Thread.sleep(6000);
            return Thread.currentThread().getName() + " : " + 7;
        };

        var taskThree = new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.WRITE, readFour);
        var taskFour = new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.READ, readSeven);

        // Group Three
        var taskGroupThree = new TaskGroup(UUID.randomUUID());

        Callable<String> readOne = () -> {
//            Thread.sleep(7000);
            return Thread.currentThread().getName() + " : " + 1;
        };
        Callable<String> readTwo = () -> {
//            Thread.sleep(8000);
            return Thread.currentThread().getName() + " : " + 2;
        };

        var taskFive = new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.WRITE, readOne);
        var taskSix = new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.READ, readTwo);

        List<Future<String>> jobs = new ArrayList<>();
        TaskExecutorService taskExecutor = new TaskExecutorService(3);
        jobs.add(taskExecutor.submitTask(taskOne));
        jobs.add(taskExecutor.submitTask(taskTwo));
        jobs.add(taskExecutor.submitTask(taskThree));
        jobs.add(taskExecutor.submitTask(taskFour));
        jobs.add(taskExecutor.submitTask(taskFive));
        jobs.add(taskExecutor.submitTask(taskSix));

        try {
            for (Future<String> job : jobs) {
                System.out.println(job.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            taskExecutor.shutdown();
        }
    }

    static class TaskExecutorService implements TaskExecutor {

        private final static int MAX_ALLOWED_THREADS = Runtime.getRuntime().availableProcessors();

        private final Set<Worker> workerQueue;

        private final int threadLimit;

        private final BlockingQueue<RunnableFuture<?>> workQueue;

        private int currentThreadCount;

        public TaskExecutorService() {
            this(MAX_ALLOWED_THREADS);
        }

        public TaskExecutorService(int numberOfThreads) {
            threadLimit = Math.min(numberOfThreads, MAX_ALLOWED_THREADS);
            workerQueue = new HashSet<>(threadLimit);
            currentThreadCount = 0;
            workQueue = new LinkedBlockingQueue<>();
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            if (task == null) {
                throw new NullPointerException("Provided task is null");
            }

            RunnableFuture<T> futureTask = new FutureTask<>(task.taskAction);
            workQueue.offer(futureTask);

            if (currentThreadCount < threadLimit) {
                currentThreadCount++;
                Worker worker = new Worker(workQueue);
                workerQueue.add(worker);
                worker.thread.start();
            }

            return futureTask;
        }

        public void shutdown() {
            for (Worker worker : workerQueue) {
                worker.thread.interrupt();
            }
        }
    }

    static class Worker implements Runnable {
        private final BlockingQueue<RunnableFuture<?>> blockingQueue;
        private final Thread thread;

        Worker(BlockingQueue<RunnableFuture<?>> blockingQueue) {
            this.blockingQueue = blockingQueue;
            this.thread = new Thread(this);
        }

        @Override
        public void run() {
            while (true) {
                RunnableFuture<?> runnable;
                try {
                    runnable = blockingQueue.take();
                } catch (InterruptedException e) {
                    break;
                }
                runnable.run();
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