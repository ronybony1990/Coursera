package concurrency.threadpools.costomthreadpool;

import java.util.concurrent.*;
import java.util.*;

public class Main {

}

class CustomPool {
    private BlockingQueue queue;
    private List<Worker> workers;
    private boolean isStopped = false;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);


    public CustomPool(int noOfThreads,
                      int maxNoOfTasks) {
        this.queue = new ArrayBlockingQueue<>(maxNoOfTasks);
        Future<?> s = executor.submit(new Worker(this.queue));
        for (int i = 0; i < noOfThreads; i++) {
            workers.add(new Worker(this.queue));
        }

        for (Worker worker: workers) {
            new Thread(worker).start();
        }
    }

    public synchronized void execute(Runnable task) {
        if (this.isStopped)
            throw new IllegalStateException("Threadpool is stopped");
        this.queue.offer(task);
    }

}

class Worker implements Runnable {

    Worker(BlockingQueue queue) {

    }

    @Override
    public void run() {

    }
}