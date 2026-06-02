package pvt.muxalma.feminine;

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class SimpleOrderingProcessor implements Consumer<NetworkEvent> {
    private static final Logger log = LoggerFactory.getLogger(SimpleOrderingProcessor.class);

    private final Consumer<NetworkEvent> downstream;
    private final Map<UUID, ConnectionState> connections = new ConcurrentHashMap<>();

    public SimpleOrderingProcessor(Consumer<NetworkEvent> downstream) {
        this.downstream = downstream;
    }
    
    @Override
    public void accept(NetworkEvent event) {
        connections.compute(event.getConnectionId(), (uuid, existing) ->
                existing == null ? new ConnectionState() : existing).addEvent(event);
    }

    private class ConnectionState {
        private int nextExpectedSerial = 0;
        private final PriorityBlockingQueue<NetworkEvent> pendingEvents = new PriorityBlockingQueue<>(
            11, Comparator.comparingInt(NetworkEvent::getSerial)
        );

        private synchronized void addEvent(NetworkEvent event) {
            if (event.getType() == EventType.ABORT) {
                downstream.accept(event);
            } else if (event.getSerial() == nextExpectedSerial) {
                downstream.accept(event);
                nextExpectedSerial++;
                processPending();
            } else if (event.getSerial() > nextExpectedSerial) {
                pendingEvents.offer(event);
            } else {
                log.warn("{}: duplicate serial = {}", event.getConnectionId(), event.getSerial());
            }
        }

        private synchronized void processPending() {
            NetworkEvent next;
            while ((next = pendingEvents.peek()) != null && next.getSerial() == nextExpectedSerial) {
                pendingEvents.poll();
                downstream.accept(next);
                nextExpectedSerial++;
            }
        }
    }
}