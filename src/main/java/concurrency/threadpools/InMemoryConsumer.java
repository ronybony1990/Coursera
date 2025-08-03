package concurrency.threadpools;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class InMemoryConsumer {

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> dataMap = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(8);
    private final Map<String, Task> activeTasks = new ConcurrentHashMap<>();
    private final Set<String> pausedKeys = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public InMemoryConsumer() {
        new Thread(this::run).start();
    }

    public void addData(String key, String data) {
        dataMap.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(data);
    }

    public void run() {
        try {
            while (!stopped.get()) {
                processData();
                try {
                    Thread.sleep(100); // Simulate polling interval
                } catch (InterruptedException e) {
                    if (!stopped.get()) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    private void processData() {
        List<String> keysToPause = new ArrayList<>();
        dataMap.forEach((key, queue) -> {
            if (!pausedKeys.contains(key) && !queue.isEmpty()) {
                List<String> records = new ArrayList<>();
                String record;
                while ((record = queue.poll()) != null) {
                    records.add(record);
                }
                if (!records.isEmpty()) {
                    Task task = new Task(records);
                    executor.submit(task);
                    activeTasks.put(key, task);
                    keysToPause.add(key);
                }
            }
        });
        pausedKeys.addAll(keysToPause);
    }

    private void checkActiveTasks() {
        List<String> finishedKeys = new ArrayList<>();
        activeTasks.forEach((key, task) -> {
            if (task.isFinished()) {
                finishedKeys.add(key);
            }
        });
        finishedKeys.forEach(key -> {
            activeTasks.remove(key);
            pausedKeys.remove(key);
        });
    }

    public void pause(String key) {
        pausedKeys.add(key);
    }

    public void resume(String key) {
        pausedKeys.remove(key);
    }

    public void stopConsuming() {
        stopped.set(true);
    }

    static class Task implements Runnable {

        private final List<String> records;
        private final AtomicBoolean finished = new AtomicBoolean(false);

        public Task(List<String> records) {
            this.records = records;
        }

        @Override
        public void run() {
            for (String record : records) {
                // Simulate record processing
                System.out.println("Processing: " + record);
                try {
                    Thread.sleep(500); // Simulate processing time
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            finished.set(true);
        }

        public boolean isFinished() {
            return finished.get();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        InMemoryConsumer consumer = new InMemoryConsumer();
        consumer.addData("key1", "data1");
        consumer.addData("key1", "data2");
        consumer.addData("key2", "data3");
        consumer.addData("key2", "data4");

        Thread.sleep(1000);
        consumer.pause("key1");
        consumer.addData("key1", "data5");
        Thread.sleep(5000);
        consumer.resume("key1");
        Thread.sleep(5000);
        consumer.stopConsuming();
    }
}
