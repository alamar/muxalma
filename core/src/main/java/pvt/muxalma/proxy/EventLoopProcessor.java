package pvt.muxalma.proxy;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class EventLoopProcessor implements Consumer<NetworkEvent> {
    private final Consumer<NetworkEvent> downstream;
    private final Map<UUID, ConnectionState> connections = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    
    public EventLoopProcessor(Consumer<NetworkEvent> downstream) {
        this.downstream = downstream;
    }
    
    @Override
    public void accept(NetworkEvent event) {
        executor.submit(() -> processEvent(event));
    }
    
    private void processEvent(NetworkEvent event) {
        if (event.getEventType() == EventType.OPEN) {
            connections.put(event.getConnectionId(), new ConnectionState());
            downstream.accept(event);
            return;
        }
        
        ConnectionState state = connections.get(event.getConnectionId());
        if (state == null && event.getEventType() != EventType.CLOSE) {
            System.err.println("Unknown connection: " + event.getConnectionId());
            return;
        }
        
        if (event.getEventType() == EventType.CLOSE) {
            connections.remove(event.getConnectionId());
            downstream.accept(event);
            return;
        }
        
        // DATA event - проверяем serial
        state.addEvent(event);
    }
    
    private class ConnectionState {
        private int nextExpectedSerial = 0;
        private final PriorityBlockingQueue<NetworkEvent> pendingEvents = new PriorityBlockingQueue<>(
            11, Comparator.comparingInt(NetworkEvent::getSerial)
        );
        
        void addEvent(NetworkEvent event) {
            synchronized (this) {
                if (event.getSerial() == nextExpectedSerial) {
                    downstream.accept(event);
                    nextExpectedSerial++;
                    processPending();
                } else if (event.getSerial() > nextExpectedSerial) {
                    pendingEvents.offer(event);
                } else {
                    System.err.println("Duplicate or old event: " + event);
                }
            }
        }
        
        private void processPending() {
            NetworkEvent next;
            while ((next = pendingEvents.peek()) != null && next.getSerial() == nextExpectedSerial) {
                pendingEvents.poll();
                downstream.accept(next);
                nextExpectedSerial++;
            }
        }
    }
    
    public void shutdown() {
        executor.shutdown();
    }
}