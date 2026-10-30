package com.taobao.drc.client;

import com.taobao.drc.client.impl.DRCClientImpl;
import com.taobao.drc.client.store.StoreClient;
import com.taobao.drc.client.enums.DBType;
import io.netty.util.concurrent.Future;
import org.junit.Test;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.Assert.*;

public class StopDuringDiscoveryTest {
    static class Client extends DRCClientImpl {
        final CountDownLatch discovering = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicInteger starts = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger stops = new AtomicInteger();
        final CountDownLatch preparing = new CountDownLatch(1), prepared = new CountDownLatch(1);
        boolean blockPreparation;
        boolean failPreparation;
        Client() throws IOException { super(new Properties()); }
        @Override protected StoreClient switchClient(boolean multi) {
            discovering.countDown();
            try { release.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
            return new StoreClient() {
                public Future writeUserCtlMessage(byte[] bytes) { return null; }
                public void stopService() { stops.incrementAndGet(); }
                public Callable<Boolean> startService() {
                    starts.incrementAndGet();
                    if (blockPreparation) {
                        preparing.countDown();
                        try { prepared.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
                    }
                    if (failPreparation) throw new IllegalStateException("CM preparation failed");
                    return () -> { calls.incrementAndGet(); return false; };
                }
                public Callable<Boolean> startMultiService() { return startService(); }
                public String getMultiSafeTimestamp() { return null; }
                public void sendRuntimeLog(String level, String message) { }
                public DBType getRunningDBType() { return null; }
            };
        }
    }

    @Test public void stopDuringDiscoveryPreventsSingleAndHashStartup() throws Exception {
        for (boolean multi : new boolean[] {false, true}) {
            Client client = new Client();
            client.addDataFilter(new DataFilter("*;*;*"));
            client.initService("test", "test", "test", "1000", null);
            Thread thread = multi ? client.startMultiService() : client.startService();
            AtomicReference<Throwable> error = new AtomicReference<Throwable>();
            thread.setUncaughtExceptionHandler((t, e) -> error.set(e));
            try {
                assertTrue(client.discovering.await(2, TimeUnit.SECONDS));
                client.stopService();
                client.release.countDown();
                thread.join(2000);
                assertFalse(thread.isAlive());
                assertNull(error.get());
                assertEquals(0, client.starts.get());
            } finally { client.release.countDown(); client.stopService(); }
        }
    }

    @Test public void blockingDrcPreparationDoesNotHoldTheStopLock() throws Exception {
        assertPreparationStopped(false);
    }

    @Test public void failedDrcPreparationAfterStopStillCleansUp() throws Exception {
        assertPreparationStopped(true);
    }

    private void assertPreparationStopped(boolean failPreparation) throws Exception {
        Client client = new Client();
        client.blockPreparation = true;
        client.failPreparation = failPreparation;
        client.release.countDown();
        client.addDataFilter(new DataFilter("*;*;*"));
        client.initService("test", "test", "test", "1000", null);
        Thread thread = client.startService();
        java.util.concurrent.ExecutorService stopping = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            assertTrue(client.preparing.await(2, TimeUnit.SECONDS));
            stopping.submit(() -> { client.stopService(); return null; }).get(2, TimeUnit.SECONDS);
            client.prepared.countDown();
            thread.join(2000);
            assertFalse(thread.isAlive());
            assertEquals(0, client.calls.get());
            // Stop and worker-finally may both request the idempotent cleanup.
            assertTrue(client.stops.get() >= 1);
        } finally {
            client.prepared.countDown();
            stopping.shutdownNow();
            client.stopService();
        }
    }
}
