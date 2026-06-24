package pvt.muxalma.processor;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.NetworkEvent;

public class TimeoutWatchdogProcessor {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    private final AtomicLong lastMessagePassed = new AtomicLong();

    public TimeoutWatchdogProcessor(String name, long maxDelayS) {
        executor.scheduleAtFixedRate(() -> {
            long passedSinceLastEventMs = System.currentTimeMillis() - lastMessagePassed.get();
            if (passedSinceLastEventMs > (maxDelayS * 1000L)) {
                log.error("More than {} s has passed since {} has seen an event - terminating process!",
                        maxDelayS, name);
                System.exit(11);
            }
        }, maxDelayS, maxDelayS, TimeUnit.SECONDS);
    }

    public Consumer<NetworkEvent> wrap(Consumer<NetworkEvent> downstream) {
        return event -> {
            lastMessagePassed.set(System.currentTimeMillis());
            downstream.accept(event);
        };
    }

    public BiConsumer<UUID, NetworkEvent> wrapH(BiConsumer<UUID, NetworkEvent> downstream) {
        return (uuid, event) -> {
            lastMessagePassed.set(System.currentTimeMillis());
            downstream.accept(uuid, event);
        };
    }

    public void shutdown() {
        executor.shutdown();
    }
}