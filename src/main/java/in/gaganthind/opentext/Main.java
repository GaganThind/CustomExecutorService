package in.gaganthind.opentext;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) {
        var i = new AtomicInteger();
        Callable<String> work = () -> {
            Thread.sleep(100);
            return Thread.currentThread().getName() + " : " + (i.incrementAndGet());
        };

        List<Task<String>> tasks = new ArrayList<>();

        // Group One
        var taskGroupOne = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.WRITE, work));
        tasks.addAll(Collections.nCopies(5, new Task<>(UUID.randomUUID(), taskGroupOne, TaskType.READ, work)));

        // Group Two
        var taskGroupTwo = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.WRITE, work));
        tasks.addAll(Collections.nCopies(5, new Task<>(UUID.randomUUID(), taskGroupTwo, TaskType.READ, work)));

        // Group Three
        var taskGroupThree = new TaskGroup(UUID.randomUUID());
        tasks.add(new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.WRITE, work));
        tasks.addAll(Collections.nCopies(5, new Task<>(UUID.randomUUID(), taskGroupThree, TaskType.READ, work)));

        // Execute the jobs
        TaskExecutorService taskExecutor = TaskExecutorService.newFixedThreadPool(6);

        tasks.stream()
                .map(taskExecutor::submitTask)
                .toList()
                .forEach(job -> {
                    try {
                        System.out.println(job.get());
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });

        taskExecutor.shutdown();
    }

    static class TaskExecutorService implements TaskExecutor {

        private final Set<Worker> workerQueue;

        private final int threadLimit;

        private final BlockingQueue<TaskWithFutureTask> workQueue;

        private int currentThreadCount;

        private final Map<String, Object> mutexMap;

        private TaskExecutorService(int numberOfThreads) {
            threadLimit = Math.min(numberOfThreads, Runtime.getRuntime().availableProcessors());
            workerQueue = new HashSet<>(threadLimit);
            currentThreadCount = 0;
            workQueue = new LinkedBlockingQueue<>();
            mutexMap = new ConcurrentHashMap<>();
        }

        public static TaskExecutorService newFixedThreadPool(int numberOfThreads) {
            return new TaskExecutorService(numberOfThreads);
        }

        @Override
        public <T> Future<T> submitTask(Task<T> task) {
            if (task == null) {
                throw new NullPointerException("Provided task is null");
            }

            mutexMap.computeIfAbsent(task.taskGroup.groupUUID.toString(), k -> new Object());

            RunnableFuture<T> futureTask = new FutureTask<>(task.taskAction);
            workQueue.offer(new TaskWithFutureTask(task, futureTask));

            addWorkerIfNeeded();

            return futureTask;
        }

        private void addWorkerIfNeeded() {
            if (currentThreadCount < threadLimit) {
                currentThreadCount++;
                Worker worker = new Worker(workQueue, mutexMap);
                workerQueue.add(worker);
                worker.thread.start();
            }
        }

        public void shutdown() {
            for (Worker worker : workerQueue) {
                worker.thread.interrupt();
            }
        }
    }

    record TaskWithFutureTask(Task<?> task, RunnableFuture<?> futureTask) {
    }

    static class Worker implements Runnable {
        private final BlockingQueue<TaskWithFutureTask> blockingQueue;
        private final Thread thread;
        private final Map<String, Object> mutexMap;

        Worker(BlockingQueue<TaskWithFutureTask> blockingQueue, Map<String, Object> mutexMap) {
            this.blockingQueue = blockingQueue;
            this.thread = new Thread(this);
            this.mutexMap = mutexMap;
        }

        @Override
        public void run() {
            while (true) {
                TaskWithFutureTask task;
                try {
                    task = blockingQueue.take();
                } catch (InterruptedException e) {
                    System.out.printf("Interrupt received for the thread %s%n", Thread.currentThread().getName());
                    break;
                }

                synchronized (mutexMap.get(task.task.taskGroup.groupUUID.toString())) {
                    System.out.printf("Started - Task %s belonging to group %s with thread %s - start time : %d%n", task.task.taskUUID, task.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
                    task.futureTask.run();
                    System.out.printf("Finished - Task %s belonging to group %s with thread %s - end time : %d%n", task.task.taskUUID, task.task.taskGroup.groupUUID, Thread.currentThread().getName(), System.currentTimeMillis());
                }
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