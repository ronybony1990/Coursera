package concurrency.threadpools.OrderedESConcMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class ResourceTrackerImpl {
    private final ConcurrentHashMap<String, Long> trackerMap = new ConcurrentHashMap<>();
    private final OrderedExecutor ex = new OrderedExecutor(Executors.newFixedThreadPool(2));
    private final AtomicLong totalSpace = new AtomicLong(0L);

    public void addResource(String requestId,
                            String clientIp,
                            Long size) {
        Runnable task = new ResourceTask(clientIp, size);
        ex.execute(task, clientIp);
    }

    public void removeResource(String requestId,
                               String clientIp,
                               Long size) {
        Runnable task = new ResourceTask(clientIp, -size);
        ex.execute(task, clientIp);
    }

    public Long getSizeByIp(String clientIp) {
        return trackerMap.getOrDefault(clientIp, 0L);
    }

    public Long getTotalSizeAcrossIps() {
        return totalSpace.get();
    }

    public void shutdown() {
        System.out.println("Shutting down...");
        ex.shutdown();
        System.out.println("executors shut down");
    }

    private class ResourceTask implements Runnable {
        private final String clientIp;
        private final Long size;

        ResourceTask(String clientIp,
                     Long size) {
            this.clientIp = clientIp;
            this.size = size;
        }

        @Override
        public void run() {
            trackerMap.compute(clientIp, (k, v) -> {
                long ipSize;
                if (v == null) ipSize =  size;
                else ipSize = v + size;
                totalSpace.addAndGet(size);
                return ipSize;
            });
        }
    }
}

class OrderedExecutor {

    private static final Queue<Runnable> EMPTY_QUEUE = new QueueWithHashCodeAndEquals<Runnable>(
            new ConcurrentLinkedQueue<Runnable>());

    private final ConcurrentMap<Object, Queue<Runnable>> taskMap = new ConcurrentHashMap<Object, Queue<Runnable>>();
    private final Executor delegate;
    private volatile boolean stopped;

    public OrderedExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    public void execute(Runnable task, Object key) {
        if (stopped) {
            System.out.println("what are you doing ? shutdown hook has been called");
            return;
        }

        // computeIfPresent attempts to retrieve the queue associated with the given key.
        // If a queue exists for the key, the provided lambda function is executed.
        Queue<Runnable> queueForKey = taskMap.computeIfPresent(key, (k, v) -> {
            v.add(task);
            return v;
        });
        // If no queue exist, then the computeIfPresent returns null.
        if (queueForKey == null) {
            // There was no running task with this key
            Queue<Runnable> newQ = new QueueWithHashCodeAndEquals<Runnable>(new ConcurrentLinkedQueue<Runnable>());
            newQ.add(task);
            // Use putIfAbsent because this execute() method can be called concurrently as well
            // attempts to put the new queue into the map,
            // but only if there isn't already a queue associated with that key
            queueForKey = taskMap.putIfAbsent(key, newQ);

            // If another thread managed to add a queue in the time between the null check and the putIfAbsent call,
            // then putIfAbsent returns the existing queue, and the current task is added to that queue.
            if (queueForKey != null)
                queueForKey.add(task);
            delegate.execute(new InternalRunnable(key));
        }
    }

    public void shutdown() {
        stopped = true;
        taskMap.clear();
        ExecutorService executorService = (ExecutorService)delegate;
        executorService.shutdown();
        try
        {
            // Wait a while for existing tasks to terminate
            if ( ! executorService.awaitTermination( 5 , TimeUnit.MINUTES ) )
            {
                executorService.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if ( ! executorService.awaitTermination( 5 , TimeUnit.MINUTES ) )
                { System.err.println( "Executor service failed to terminate." ); }
            }
        }
        catch ( InterruptedException ex )
        {
            executorService.shutdownNow();       // (Re-)Cancel if current thread also interrupted
            Thread.currentThread().interrupt();           // Preserve interrupt status
        }
    }

    /**
     * Own Runnable used by OrderedExecutor.
     * The runnable is associated with a specific key - the Queue&lt;Runnable> for this
     * key is polled.
     * If the queue is empty, it tries to remove the queue from taskMap.
     *
     */
    private class InternalRunnable implements Runnable {

        private Object key;

        public InternalRunnable(Object key) {
            this.key = key;
        }

        @Override
        public void run() {
            while (true) {
                // There must be at least one task now
                Runnable r = taskMap.get(key).poll();
                while (r != null) {
                    r.run();
                    r = taskMap.get(key).poll();
                }
                // The queue emptied
                // Remove from the map if and only if the queue is really empty
                boolean removed = taskMap.remove(key, EMPTY_QUEUE);
                if (removed) {
                    // The queue has been removed from the map,
                    // if a new task arrives with the same key, a new InternalRunnable
                    // will be created
                    break;
                } // If the queue has not been removed from the map it means that someone put a task into it
                // so we can safely continue the loop
            }
        }
    }

    /**
     * Special Queue implementation, with equals() and hashCode() methods.
     * By default, Java SE queues use identity equals() and default hashCode() methods.
     * This implementation uses Arrays.equals(Queue::toArray()) and Arrays.hashCode(Queue::toArray()).
     *
     * @param <E> The type of elements in the queue.
     */
    private static class QueueWithHashCodeAndEquals<E> implements Queue<E> {

        private Queue<E> delegate;

        public QueueWithHashCodeAndEquals(Queue<E> delegate) {
            this.delegate = delegate;
        }

        public boolean add(E e) {
            return delegate.add(e);
        }

        public boolean offer(E e) {
            return delegate.offer(e);
        }

        public int size() {
            return delegate.size();
        }

        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        public E remove() {
            return delegate.remove();
        }

        public E poll() {
            return delegate.poll();
        }

        public E element() {
            return delegate.element();
        }

        public Iterator<E> iterator() {
            return delegate.iterator();
        }

        public E peek() {
            return delegate.peek();
        }

        public Object[] toArray() {
            return delegate.toArray();
        }

        public <T> T[] toArray(T[] a) {
            return delegate.toArray(a);
        }

        public boolean remove(Object o) {
            return delegate.remove(o);
        }

        public boolean containsAll(Collection<?> c) {
            return delegate.containsAll(c);
        }

        public boolean addAll(Collection<? extends E> c) {
            return delegate.addAll(c);
        }

        public boolean removeAll(Collection<?> c) {
            return delegate.removeAll(c);
        }

        public boolean retainAll(Collection<?> c) {
            return delegate.retainAll(c);
        }

        public void clear() {
            delegate.clear();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof QueueWithHashCodeAndEquals)) {
                return false;
            }
            QueueWithHashCodeAndEquals<?> other = (QueueWithHashCodeAndEquals<?>) obj;
            return Arrays.equals(toArray(), other.toArray());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(toArray());
        }

    }

}


class Main {
    public static void main (String[] args) {
        ResourceTrackerImpl resourceTracker = new ResourceTrackerImpl();
        resourceTracker.addResource("some-req1", "10.134.56.87", 100L);
        resourceTracker.addResource("some-req2", "10.134.56.87", 300L);
        resourceTracker.addResource("some-req3", "10.124.56.87", 200L);
        resourceTracker.removeResource("some-req1", "10.134.56.87", 100L);
        resourceTracker.addResource("some-req4", "10.124.56.87", 400L);

        try {
            Thread.sleep(2000);
        }
        catch(Exception e ) {
            System.out.println("exception: " + e.toString());
        }
        System.out.println(resourceTracker.getSizeByIp("10.134.56.87"));
        System.out.println(resourceTracker.getSizeByIp("10.124.56.87"));
        System.out.println(resourceTracker.getTotalSizeAcrossIps());

        resourceTracker.shutdown();
        resourceTracker.addResource("some-req1", "10.134.56.87", 1000L);
        System.out.println(resourceTracker.getSizeByIp("10.134.56.87"));
    }
}