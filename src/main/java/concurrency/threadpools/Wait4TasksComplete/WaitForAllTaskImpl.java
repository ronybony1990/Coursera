package concurrency.threadpools.Wait4TasksComplete;

import java.util.*;
import java.util.UUID;
import java.util.concurrent.*;

public class WaitForAllTaskImpl {
    ExecutorService es = Executors.newFixedThreadPool(3);
    List<Result> results = Collections.synchronizedList(new ArrayList<>());

    WaitForAllTaskImpl(){}

    public void executeTasksInvokeAll() {
        List<TaskInvokeAll> tasks = new ArrayList<>();
        tasks.add(new TaskInvokeAll());
        tasks.add(new TaskInvokeAll());

        List<Future<Result>> futures;
        try {
            futures = es.invokeAll(tasks, 60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        shutdownAndAwaitTermination();
        processFutures(futures);
    }

    private void processFutures(List<Future<Result>> futures) {
        List<Future<Result>> failedFutures =
                futures.stream().filter( future -> {
                            try {
                                return future.isCancelled() || ! future.isDone() || Objects.isNull( future.get() ) || Objects.equals( future.get().isSuccess , Boolean.FALSE );
                            }
                            catch ( InterruptedException e ) { throw new RuntimeException( e ); }
                            catch ( ExecutionException e ) { throw new RuntimeException( e ); }
                        } )
                        .toList();
    }

    private void shutdownAndAwaitTermination ()
    {
        es.shutdown(); // Disable new tasks from being submitted
        try
        {
            // Wait a while for existing tasks to terminate
            if ( ! es.awaitTermination( 5 , TimeUnit.MINUTES ) )
            {
                es.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if ( ! es.awaitTermination( 5 , TimeUnit.MINUTES ) )
                { System.err.println( "Executor service failed to terminate." ); }
            }
        }
        catch ( InterruptedException ex )
        {
            es.shutdownNow();       // (Re-)Cancel if current thread also interrupted
            Thread.currentThread().interrupt();           // Preserve interrupt status
        }
    }
}

class Result {
    UUID uuid;
    boolean isSuccess;

    Result(UUID uuid, boolean isSuccess) {
        this.uuid = uuid;
        this.isSuccess = isSuccess;
    }
}

class TaskInvokeAll implements Callable<Result> {
    @Override
    public Result call() {
        try {
            Thread.sleep( ThreadLocalRandom.current().nextInt( 0 , 5 ) );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }  // Pretend this method takes a while to do its work.
        return new Result(UUID.randomUUID(), true);
    }
}
