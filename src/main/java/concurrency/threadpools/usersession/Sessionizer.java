package concurrency.threadpools.usersession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

// Represents a log event.  Uses Instant for time, and Gson for parsing.
class LogEvent {
    private String userId;
    private Instant accessTime;
    private String object;

    // Constructor.
    public LogEvent(String userId, Instant accessTime, String object) {
        this.userId = userId;
        this.accessTime = accessTime;
        this.object = object;
    }

    // Creates a LogEvent from a JSON string.
    public static LogEvent fromJson(String json) throws JsonSyntaxException, DateTimeParseException {
        Gson gson = new Gson();
        Map<String, String> map = gson.fromJson(json, Map.class); // Use Map for simpler parsing
        if (map == null) {
            throw new JsonSyntaxException("Input JSON is empty or not a valid JSON object");
        }
        String userId = map.get("userId");
        String accessTimeStr = map.get("accessTime");
        String object = map.get("object");

        if (userId == null || accessTimeStr == null || object == null) {
            throw new JsonSyntaxException("Missing required field: userId, accessTime, or object");
        }

        Instant accessTime;
        try {
            accessTime = Instant.parse(accessTimeStr);
        } catch (DateTimeParseException e) {
            throw new DateTimeParseException("Invalid date/time format.  Use ISO-8601 format", accessTimeStr, 0);
        }

        return new LogEvent(userId, accessTime, object);
    }

    public String getUserId() {
        return userId;
    }

    public Instant getAccessTime() {
        return accessTime;
    }

    public String getObject() {
        return object;
    }

    public Instant getExpirationTime() {
        return accessTime.plus(Duration.ofMinutes(30));
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "userId='" + userId + '\'' +
                ", accessTime=" + accessTime +
                ", object='" + object + '\'' +
                "}";
    }
}

// Represents a user session.
class UserSession {
    private String userId;
    private List<LogEvent> events;
    private Instant sessionStart; // Keep track of session start time

    public UserSession(String userId) {
        this.userId = userId;
        this.events = new ArrayList<>();
        this.sessionStart = null; // Initialize to null, set on first event.
    }

    public String getUserId() {
        return userId;
    }

    public List<LogEvent> getEvents() {
        return events;
    }

    // Method to add event.  Handles the 30-minute window.  Returns boolean
    public boolean addEvent(LogEvent event) {
        if (this.sessionStart == null) {
            this.sessionStart = event.getAccessTime();
            this.events.add(event);
            return true; // Added first event.
        }

        Duration duration = Duration.between(this.sessionStart, event.getAccessTime());
        if (duration.toMinutes() <= 30) {
            this.events.add(event);
            return true; // Added within time window
        } else {
            return false; // Did not add, outside time window.
        }
    }

    public Instant getSessionStart() {
        return sessionStart;
    }

    public void setSessionStart(Instant sessionStart) {
        this.sessionStart = sessionStart;
    }

    public void removeExpiredEvents(Instant now) {
        events.removeIf(event -> event.getAccessTime().isBefore(now));
    }

    @Override
    public String toString() {
        return "UserSession{" +
                "userId='" + userId + '\'' +
                ", events=" + events +
                ", sessionStart=" + sessionStart + //Added session start
                "}";
    }
}

// Main class to process log events and compose sessions.
class NonRollingSessionizer {

    private final Map<String, UserSession> sessions = new ConcurrentHashMap<>(); // Use ConcurrentHashMap for thread safety.
    private final ExecutorService executorService;
    private final int numThreads;

    // Constructor.  Takes the number of threads to use.
    public NonRollingSessionizer(int numThreads) {
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);
    }

    // Adds a log event to the appropriate session.  This is the core logic.
    public void addLogEvent(LogEvent event) {
        executorService.submit(() -> { // Use the thread pool.
            String userId = event.getUserId();
            sessions.compute(userId, (key, session) -> {
                if (session == null) {
                    UserSession newSession = new UserSession(userId);
                    newSession.addEvent(event); // Add the event.
                    return newSession;
                } else {
                    // Important:  Check if the event fits in the *current* session.
                    if (!session.addEvent(event)) {
                        // If it doesn't fit, start a new session.
                        UserSession newSession = new UserSession(userId);
                        newSession.addEvent(event); // Add the event to the new session
                        return newSession; // Return the *new* session.
                    }
                    return session; // Return the *existing* session if event fits.
                }
            });
        });
    }

    // Processes the log data from a String.  Handles line-by-line processing.
    public void processLogData(String logData) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(logData))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) { //handle empty lines.
                    try {
                        LogEvent event = LogEvent.fromJson(line);
                        addLogEvent(event);
                    } catch (JsonSyntaxException | DateTimeParseException e) {
                        System.err.println("Error parsing log line: " + line + " - " + e.getMessage());
                        // Consider logging to a file or using a more robust error handling mechanism.
                        //  e.printStackTrace(); // Don't print stack traces in production.
                    }
                }
            }
        }
    }

    // Retrieves all the sessions.  Returns a COPY to prevent external modification.
    public List<UserSession> getSessions() {
        // Create a new list and add copies of the sessions.
        List<UserSession> sessionList = new ArrayList<>();
        for (UserSession session : sessions.values()) {
            // Create a new UserSession object with a copy of the events list.
            UserSession sessionCopy = new UserSession(session.getUserId());
            sessionCopy.setSessionStart(session.getSessionStart()); // Copy the session start time
            sessionCopy.getEvents().addAll(session.getEvents()); // Add a *copy* of the events
            sessionList.add(sessionCopy);
        }
        return sessionList;
    }

    // Shuts down the executor service.  Call this when you're done processing.
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("ExecutorService did not terminate in 60 seconds");
                executorService.shutdownNow(); //force shutdown
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for ExecutorService to terminate");
            executorService.shutdownNow(); //force shutdown
            Thread.currentThread().interrupt(); // Restore the interrupted status
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Example log data (you would replace this with your actual log data source).
        String logData = "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:00:00Z\",\"object\":\"objectA\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:05:00Z\",\"object\":\"objectB\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:02:00Z\",\"object\":\"objectC\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:35:00Z\",\"object\":\"objectD\"}\n" + // New session for user1
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:30:00Z\",\"object\":\"objectE\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T11:01:00Z\",\"object\":\"objectF\"}\n" + // New session for user2
                "{\"userId\":\"user3\",\"accessTime\":\"2023-10-26T12:00:00Z\",\"object\":\"objectG\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:40:00Z\",\"object\":\"objectH\"}"; // New session for user1
        // Create a Sessionizer with a thread pool size of 4.  Adjust as needed.
        NonRollingSessionizer sessionizer = new NonRollingSessionizer(4);
        try {
            sessionizer.processLogData(logData);

            // Wait for a short time to allow processing to complete.  In a real application,
            // you would use a more robust mechanism like a CountDownLatch or similar
            // to ensure all events are processed before retrieving sessions.  For this
            // example, we'll just sleep.  DON'T DO THIS IN PRODUCTION.
            Thread.sleep(1000); // 1 second.  Replace with proper synchronization.

            // Get the sessions.
            List<UserSession> sessions = sessionizer.getSessions();

            // Print the sessions.
            for (UserSession session : sessions) {
                System.out.println(session);
            }
        } finally {
            sessionizer.shutdown(); // Shut down the executor service.
        }
    }
}


// Main class to process log events and compose sessions.
class RollingSessionizer {

    private final Map<String, UserSession> sessions = new ConcurrentHashMap<>(); // Use ConcurrentHashMap for thread safety.
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1); // For the background task
    private final int numThreads;
    private final ExecutorService executorService;

    // Constructor.  Takes the number of threads to use.
    public RollingSessionizer(int numThreads) {
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);

        // Schedule the task to remove expired events every 10 seconds.
        scheduler.scheduleAtFixedRate(this::cleanupSessions, 10, 10, TimeUnit.SECONDS);
    }

    // Adds a log event to the appropriate session.  This is the core logic.
    public void addLogEvent(LogEvent event) {
        executorService.submit(() -> { // Use the thread pool.
            String userId = event.getUserId();
            sessions.compute(userId, (key, session) -> {
                if (session == null) {
                    UserSession newSession = new UserSession(userId);
                    newSession.addEvent(event); // Add the event.
                    return newSession;
                } else {
                    session.addEvent(event);
                    return session;
                }
            });
        });
    }

    // Processes the log data from a String.  Handles line-by-line processing.
    public void processLogData(String logData) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(logData))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) { //handle empty lines.
                    try {
                        LogEvent event = LogEvent.fromJson(line);
                        addLogEvent(event);
                    } catch (JsonSyntaxException | DateTimeParseException e) {
                        System.err.println("Error parsing log line: " + line + " - " + e.getMessage());
                        // Consider logging to a file or using a more robust error handling mechanism.
                        //  e.printStackTrace(); // Don't print stack traces in production.
                    }
                }
            }
        }
    }

    // Method to cleanup sessions.  This is called by the scheduler.
    private void cleanupSessions() {
        try {
            for (UserSession session : sessions.values()) {
                session.removeExpiredEvents(Instant.now());
                if (session.getEvents().isEmpty()) {
                    sessions.remove(session.getUserId());
                }
            }
        } catch (Exception e) {
            System.err.println("Error during session cleanup: " + e.getMessage());
            //  e.printStackTrace(); //don't use this in production
        }
    }

    // Retrieves all the sessions.  Returns a COPY to prevent external modification.
    public List<UserSession> getSessions() {
        // Create a new list and add copies of the sessions.
        List<UserSession> sessionList = new ArrayList<>();
        for (UserSession session : sessions.values()) {
            // Create a new UserSession object with a copy of the events list.
            UserSession sessionCopy = new UserSession(session.getUserId());
            sessionCopy.setSessionStart(session.getSessionStart());
            sessionCopy.getEvents().addAll(new ArrayList<>(session.getEvents()));
            sessionList.add(sessionCopy);
        }
        return sessionList;
    }

    // Shuts down the executor service and the scheduler.  Call this when you're done processing.
    public void shutdown() {
        executorService.shutdown();
        scheduler.shutdown(); // Shutdown the scheduler.
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("ExecutorService did not terminate in 60 seconds");
                executorService.shutdownNow(); //force shutdown
            }
            if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("ScheduledExecutorService did not terminate in 60 seconds");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for ExecutorService or ScheduledExecutorService to terminate");
            executorService.shutdownNow(); //force shutdown
            scheduler.shutdownNow();
            Thread.currentThread().interrupt(); // Restore the interrupted status
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Example log data (you would replace this with your actual log data source).
        String logData = "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:00:00Z\",\"object\":\"objectA\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:05:00Z\",\"object\":\"objectB\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:02:00Z\",\"object\":\"objectC\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:35:00Z\",\"object\":\"objectD\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:30:00Z\",\"object\":\"objectE\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T11:01:00Z\",\"object\":\"objectF\"}\n" +
                "{\"userId\":\"user3\",\"accessTime\":\"2023-10-26T12:00:00Z\",\"object\":\"objectG\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:40:00Z\",\"object\":\"objectH\"}";
        // Create a Sessionizer with a thread pool size of 4.  Adjust as needed.
        RollingSessionizer sessionizer = new RollingSessionizer(4);
        try {
            sessionizer.processLogData(logData);

            // Wait for a short time to allow processing to complete.  In a real application,
            // you would use a more robust mechanism.
            Thread.sleep(3000);

            // Get the sessions.
            List<UserSession> sessions = sessionizer.getSessions();

            // Print the sessions.
            for (UserSession session : sessions) {
                System.out.println(session);
            }
        } finally {
            sessionizer.shutdown(); // Shut down the executor service and scheduler.
        }
    }
}


class PriorityQueueSessionizer {

    private final Map<String, UserSession> sessions = new ConcurrentHashMap<>(); // Use ConcurrentHashMap for thread safety.
    private final PriorityBlockingQueue<LogEvent> expirationQueue = new PriorityBlockingQueue<>(100,
            (e1, e2) -> e1.getExpirationTime().compareTo(e2.getExpirationTime()));
    private final ExecutorService executorService;
    private final ExecutorService cleanupExecutor = Executors.newSingleThreadExecutor(); //handles cleanup
    private final int numThreads;
    private volatile boolean running = true;

    // Constructor.  Takes the number of threads to use.
    public PriorityQueueSessionizer(int numThreads) {
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);
        startCleanupThread();
    }

    // Adds a log event to the appropriate session.  This is the core logic.
    public void addLogEvent(LogEvent event) {
        executorService.submit(() -> { // Use the thread pool.
            String userId = event.getUserId();
            sessions.compute(userId, (key, session) -> {
                if (session == null) {
                    UserSession newSession = new UserSession(userId);
                    newSession.addEvent(event); // Add the event.
                    expirationQueue.add(event);
                    return newSession;
                } else {
                    session.addEvent(event);
                    expirationQueue.add(event);
                    return session;
                }
            });
        });
    }

    // Processes the log data from a String.  Handles line-by-line processing.
    public void processLogData(String logData) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(logData))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) { //handle empty lines.
                    try {
                        LogEvent event = LogEvent.fromJson(line);
                        addLogEvent(event);
                    } catch (JsonSyntaxException | DateTimeParseException e) {
                        System.err.println("Error parsing log line: " + line + " - " + e.getMessage());
                        // Consider logging to a file or using a more robust error handling mechanism.
                        //  e.printStackTrace(); // Don't print stack traces in production.
                    }
                }
            }
        }
    }



    // Starts the cleanup thread.
    private void startCleanupThread() {
        cleanupExecutor.submit(() -> {
            while (running) {
                try {
                    LogEvent expiredEvent = expirationQueue.poll(10, TimeUnit.SECONDS); // Use poll
                    if (expiredEvent != null) {
                        Instant now = Instant.now();
                        UserSession session = sessions.get(expiredEvent.getUserId()); //get session
                        if (session != null) {
                            session.removeExpiredEvents(now); //remove expired events
                            if (session.getEvents().isEmpty()) {
                                sessions.remove(session.getUserId());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                    System.err.println("Cleanup thread interrupted: " + e.getMessage());
                    running = false; //stop
                }            }
        });
    }

    // Retrieves all the sessions.  Returns a COPY to prevent external modification.
    public List<UserSession> getSessions() {
        // Create a new list and add copies of the sessions.
        List<UserSession> sessionList = new ArrayList<>();
        for (UserSession session : sessions.values()) {
            // Create a new UserSession object with a copy of the events list.
            UserSession sessionCopy = new UserSession(session.getUserId());
            sessionCopy.setSessionStart(session.getSessionStart());
            sessionCopy.getEvents().addAll(new ArrayList<>(session.getEvents()));
            sessionList.add(sessionCopy);
        }
        return sessionList;
    }

    // Shuts down the executor service and the scheduler.  Call this when you're done processing.
    public void shutdown() {
        running = false;
        executorService.shutdown();
        cleanupExecutor.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("ExecutorService did not terminate in 60 seconds");
                executorService.shutdownNow();
            }
            if (!cleanupExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("CleanupExecutorService did not terminate in 60 seconds");
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for ExecutorService or CleanupExecutorService to terminate");
            executorService.shutdownNow();
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        // Example log data (you would replace this with your actual log data source).
        String logData = "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:00:00Z\",\"object\":\"objectA\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:05:00Z\",\"object\":\"objectB\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:02:00Z\",\"object\":\"objectC\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:35:00Z\",\"object\":\"objectD\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T10:30:00Z\",\"object\":\"objectE\"}\n" +
                "{\"userId\":\"user2\",\"accessTime\":\"2023-10-26T11:01:00Z\",\"object\":\"objectF\"}\n" +
                "{\"userId\":\"user3\",\"accessTime\":\"2023-10-26T12:00:00Z\",\"object\":\"objectG\"}\n" +
                "{\"userId\":\"user1\",\"accessTime\":\"2023-10-26T10:40:00Z\",\"object\":\"objectH\"}";
        // Create a Sessionizer with a thread pool size of 4.  Adjust as needed.
        PriorityQueueSessionizer sessionizer = new PriorityQueueSessionizer(4);
        try {
            sessionizer.processLogData(logData);

            // Wait for a short time to allow processing to complete.  In a real application,
            // you would use a more robust mechanism.
            Thread.sleep(3000);

            // Get the sessions.
            List<UserSession> sessions = sessionizer.getSessions();

            // Print the sessions.
            for (UserSession session : sessions) {
                System.out.println(session);
            }
        } finally {
            sessionizer.shutdown(); // Shut down the executor service and scheduler.
        }
    }
}
