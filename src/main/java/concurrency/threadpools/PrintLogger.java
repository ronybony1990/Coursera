package concurrency.threadpools;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

// Logger Rate Limiter: Design a logging system that processes incoming messages with timestamps,
// ensuring each unique message is printed at most once every 10 seconds.
// Messages arrive in chronological order, and multiple messages may have the same timestamp.
// Implement a Logger class with a method to determine if a message should be printed.

public class PrintLogger {
}

class LogEvent {
    private final String messageId;
    private final int timestamp;
    private final String message;

    public LogEvent(String messageId, int timestamp, String msg) {
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.message = msg;
    }

    public int getTimestamp() {
        return this.timestamp;
    }

    public String getMsgId() {
        return this.messageId;
    }
}

class GCOptimizedLogger {

    private static class Event {
        AtomicInteger lastPrinted = new AtomicInteger(0);
    }

    private final ConcurrentHashMap<String, Event> map = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String>[] wheels;
    private final int interval;
    private final int tick;
    private final int wheelSize;
    private final ScheduledExecutorService es;
    private volatile int currentSlot = 0;

    public GCOptimizedLogger(int interval, int tick) {
        this.tick =tick;
        this.interval = interval;
        this.es = Executors.newSingleThreadScheduledExecutor( r -> {
            Thread thread = new Thread(r, "cleaner");
            thread.setDaemon(true);
            return thread;
        });
        this.wheelSize = (interval / tick) + 1;
        this.wheels = new ConcurrentLinkedQueue[wheelSize];
        for (int i = 0; i < this.wheelSize; i++) {
            this.wheels[i] = new ConcurrentLinkedQueue<>();
        }
        es.scheduleAtFixedRate(this::cleanup, tick, tick, TimeUnit.SECONDS);
    }

    //@SuppressWarnings("unchecked")
    public boolean shouldPrintMsg(LogEvent event) {
        Event entry = map.computeIfAbsent(event.getMsgId(), k -> new Event());

        while(true) {
            int oldTime = entry.lastPrinted.get();
            if (event.getTimestamp() - oldTime >= interval) {
                if (entry.lastPrinted.compareAndSet(oldTime, event.getTimestamp())) {
                    // Schedule cleanup after interval seconds
                    int targetSlot = (currentSlot + (interval / tick)) % wheelSize;
                    wheels[targetSlot].add(event.getMsgId());
                    return true;
                }
                // CAS failed-> retry
            } else {
                return false;
            }
        }
    }

    private void cleanup() {
        currentSlot = (currentSlot + 1) % wheelSize;
        ConcurrentLinkedQueue<String> slot = wheels[currentSlot];
        while (!slot.isEmpty()) {
            String msgId = slot.poll();
            map.remove(msgId);
        }
    }
}

