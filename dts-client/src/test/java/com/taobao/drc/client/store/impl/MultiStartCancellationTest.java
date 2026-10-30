package com.taobao.drc.client.store.impl;

import com.sun.net.httpserver.HttpServer;
import com.taobao.drc.client.SubscribeChannel;
import com.taobao.drc.client.config.UserConfig;
import com.taobao.drc.client.impl.Checkpoint;
import com.taobao.drc.client.checkpoint.CheckpointManager;
import org.junit.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.*;

public class MultiStartCancellationTest {
    @Test public void rejectedSecondSubmissionStillWaitsForAlreadyStartedShard() throws Exception {
        CountDownLatch firstRunning = new CountDownLatch(1), releaseFirst = new CountDownLatch(1);
        CountDownLatch secondSubmit = new CountDownLatch(1), rejectSecond = new CountDownLatch(1);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>()) {
            AtomicInteger submissions = new AtomicInteger();
            @Override public void execute(Runnable task) {
                if (submissions.incrementAndGet() == 1) {
                    super.execute(() -> {
                        firstRunning.countDown();
                        boolean interrupted = false;
                        while (releaseFirst.getCount() != 0) {
                            try { releaseFirst.await(); } catch (InterruptedException e) { interrupted = true; }
                        }
                        if (interrupted) { Thread.currentThread().interrupt(); }
                        task.run();
                    });
                } else {
                    secondSubmit.countDown();
                    try { rejectSecond.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
                    super.execute(task);
                }
            }
        };
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = "{\"isSuccess\":true,\"data\":{\"topics\":[\"a\",\"b\"]}}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length); exchange.getResponseBody().write(body); exchange.close();
        });
        server.start(); ExecutorService caller = Executors.newSingleThreadExecutor();
        UserConfig config = new UserConfig(); config.setCheckpoint(new Checkpoint()); config.getCheckpoint().setTimestamp("1000");
        config.setSubscribeChannel(SubscribeChannel.DTS); config.setMultiMode(true); config.setDb("test");
        config.setClusterUrl("http://127.0.0.1:" + server.getAddress().getPort());
        DStoreClientImpl client = new DStoreClientImpl(null, config, new CheckpointManager(true), null, null, null) {
            @Override protected ExecutorService createMultiServiceExecutor(int size, String prefix) { return executor; }
        };
        try {
            Future<Boolean> run = caller.submit(client.startMultiService());
            assertTrue(firstRunning.await(2, TimeUnit.SECONDS)); assertTrue(secondSubmit.await(2, TimeUnit.SECONDS));
            client.stopService(); rejectSecond.countDown();
            try { run.get(200, TimeUnit.MILLISECONDS); fail("Returned while old shard task was still running"); }
            catch (TimeoutException expected) { }
            releaseFirst.countDown();
            try { run.get(3, TimeUnit.SECONDS); fail("Expected rejected submission"); }
            catch (ExecutionException expected) { assertTrue(expected.getCause() instanceof RejectedExecutionException); }
            assertTrue(executor.isTerminated());
        } finally {
            releaseFirst.countDown(); rejectSecond.countDown(); client.stopService(); caller.shutdownNow(); executor.shutdownNow(); server.stop(0);
        }
    }
}
