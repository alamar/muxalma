package pvt.muxalma.fanservice;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class PollingRetrievalSupport {
    private final long pollIntervalMs;
    private final ExecutorService executorService;
    private volatile boolean running = false;
    private Future<?> pollingTask;

    protected PollingRetrievalSupport(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
        executorService = Executors.newCachedThreadPool();
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        pollingTask = executorService.submit(() -> {
            while (running) {
                try {
                    poll();
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    protected abstract void poll();

    public void stop() {
        running = false;
        if (pollingTask != null) {
            pollingTask.cancel(true);
        }
    }

}
