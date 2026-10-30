package com.taobao.drc.client;

import com.taobao.drc.client.impl.DRCClientImpl;
import com.taobao.drc.client.store.StoreClient;
import com.taobao.drc.client.enums.DBType;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import io.netty.util.concurrent.Future;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

public class ClientRestartTest {
    static class Store implements StoreClient {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicInteger starts = new AtomicInteger(), stops = new AtomicInteger();
        public Future writeUserCtlMessage(byte[] bytes) { return null; }
        public void stopService() { stops.incrementAndGet(); release.countDown(); }
        public Callable<Boolean> startService() {
            starts.incrementAndGet();
            return () -> { entered.countDown(); release.await(); return false; };
        }
        public Callable<Boolean> startMultiService() { return startService(); }
        public String getMultiSafeTimestamp() { return "1200"; }
        public void sendRuntimeLog(String level, String message) { }
        public DBType getRunningDBType() { return DBType.MYSQL; }
    }
    static class Client extends DRCClientImpl {
        final Store store;
        Client(Store store) throws Exception { this(store, false); }
        Client(Store store, boolean autoRetry) throws Exception {
            super(new Properties()); this.store = store;
            addDataFilter(new DataFilter("*;*;*"));
            initService("test", "test", "test", "1000", null);
            if (!autoRetry) { shutdownAutoRetry(); }
        }
        @Override protected StoreClient switchClient(boolean multi) { return store; }
    }
    @Test public void manualJoinResetStartLoopRemainsUsable() throws Exception {
        Store store = new Store(); store.release.countDown(); Client client = new Client(store);
        Thread first = client.startService(); first.join(2000); assertFalse(first.isAlive());
        client.resetService(); client.resetService();
        Thread second = client.startService(); second.join(2000); assertFalse(second.isAlive());
        assertEquals(2, store.starts.get()); assertTrue(store.stops.get() >= 2);
        assertEquals("1200", client.getMultiSafeTimestamp());
    }
    @Test public void runningResetAndChangingSubscriptionContractAreRejected() throws Exception {
        Store store = new Store(); Client client = new Client(store); Thread worker = client.startService();
        try {
            assertTrue(store.entered.await(2, TimeUnit.SECONDS));
            try { client.resetService(); fail("Running reset must fail"); } catch (IllegalStateException expected) { }
        } finally { client.stopService(); }
        assertFalse(worker.isAlive());
        client.resetService();
        try { client.startMultiService(); fail("Cannot change the preserved session's contract"); }
        catch (IllegalStateException expected) { }
    }
    @Test public void externalStopWaitsForRootWorkerAndKeepsSafeState() throws Exception {
        Store store = new Store(); Client client = new Client(store); Thread worker = client.startService();
        assertTrue(store.entered.await(2, TimeUnit.SECONDS));
        client.stopService();
        assertFalse(worker.isAlive()); assertEquals("1200", client.getMultiSafeTimestamp());
    }
    @Test public void concurrentStartsCreateExactlyOneRootWorker() throws Exception {
        Store store = new Store(); Client client = new Client(store);
        ExecutorService callers = Executors.newFixedThreadPool(8); CountDownLatch go = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger(); List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < 8; i++) futures.add(callers.submit(() -> {
                go.await(); try { client.startService(); successes.incrementAndGet(); } catch (Exception expected) { }
                return null;
            }));
            go.countDown(); for (java.util.concurrent.Future<?> future : futures) future.get(2, TimeUnit.SECONDS);
            assertEquals(1, successes.get());
        } finally { client.stopService(); callers.shutdownNow(); }
    }

    @Test public void trackingFailureTerminatesEvenWithAutomaticRetryEnabled() throws Exception {
        final RuntimeException error = new com.taobao.drc.client.checkpoint.CheckpointTrackingException("budget exceeded");
        Store store = new Store() {
            @Override public Callable<Boolean> startService() { starts.incrementAndGet(); return () -> { throw error; }; }
        };
        Client client = new Client(store, true);
        Thread worker = client.startMultiService();
        worker.setUncaughtExceptionHandler((thread, failure) -> { });
        try {
            worker.join(2000); assertFalse("Terminal errors must not enter the retry sleep", worker.isAlive());
            assertSame(error, client.getLastFailure()); assertEquals(1, store.starts.get());
        } finally { client.stopService(); }
    }

    @Test public void stopInsideRealNotifyDoesNotWaitForCoordinator() throws Exception {
        java.util.concurrent.atomic.AtomicReference<Client> reference = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        CountDownLatch callbackExited = new CountDownLatch(1);
        Store store = new Store() {
            @Override public Callable<Boolean> startService() {
                return () -> {
                    Thread callback = new Thread(() -> {
                        com.taobao.drc.client.config.UserConfig config = new com.taobao.drc.client.config.UserConfig();
                        config.setCheckpoint(new com.taobao.drc.client.impl.Checkpoint());
                        Listener listener = new Listener() {
                            public void notify(com.taobao.drc.client.message.DataMessage message) throws Exception { reference.get().stopService(); }
                            public void notifyRuntimeLog(String level, String message) { }
                            public void handleException(Exception failure) { }
                        };
                        try {
                            com.taobao.drc.client.message.DataMessage.Record record = new com.taobao.drc.client.message.DataMessage.Record();
                            record.setType(com.taobao.drc.client.message.DataMessage.Record.Type.HEARTBEAT);
                            record.addAttribute("timestamp", "1100"); record.addAttribute("source_type", "mysql");
                            new com.taobao.drc.client.network.RecordNotifyHelper(config, new CheckpointManager(false), listener).process(record);
                        } catch (Throwable failure) { error.set(failure); }
                        finally { callbackExited.countDown(); }
                    });
                    callback.setDaemon(true); callback.start();
                    try { callback.join(); } catch (InterruptedException expected) { Thread.currentThread().interrupt(); }
                    return false;
                };
            }
        };
        Client client = new Client(store); reference.set(client); Thread worker = client.startService();
        assertTrue(callbackExited.await(2, TimeUnit.SECONDS)); worker.join(2000);
        assertFalse(worker.isAlive()); assertNull(error.get());
    }
}
