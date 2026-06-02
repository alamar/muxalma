package pvt.muxalma.processor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import pvt.muxalma.model.NetworkEvent;

public class ParallelProcessor implements Consumer<NetworkEvent> {
    private final Consumer<NetworkEvent> downstream;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    public ParallelProcessor(Consumer<NetworkEvent> downstream) {
        this.downstream = downstream;
    }

    @Override
    public void accept(NetworkEvent event) {
        executor.submit(() -> processEvent(event));
    }

    private void processEvent(NetworkEvent event) {
        downstream.accept(event);
    }

    public void shutdown() {
        executor.shutdown();
    }
}