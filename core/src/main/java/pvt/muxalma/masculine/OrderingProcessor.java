package pvt.muxalma.masculine;

import java.util.Comparator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

import pvt.muxalma.model.NetworkEvent;

public class OrderingProcessor implements Consumer<NetworkEvent> {
    private final Consumer<? super ConnectionEvent> downstream;
    private final Map<UUID, ConnectionState> connections = new ConcurrentHashMap<>();
    // TODO handle stale connections? See shutdown() below
    //private final ExecutorService executor = Executors.newCachedThreadPool();

    public OrderingProcessor(Consumer<? super ConnectionEvent> downstream) {
        this.downstream = downstream;
    }
    
    @Override
    public void accept(NetworkEvent event) {
        connections.compute(event.getConnectionId(), (uuid, existing) ->
                existing == null ? new ConnectionState() : existing).addEvent(event);
    }

    public class ConnectionState {
        private boolean isOpen = false;
        private int nextExpectedSerial = 0;
        private final PriorityBlockingQueue<NetworkEvent> pendingEvents = new PriorityBlockingQueue<>(
            11, Comparator.comparingInt(NetworkEvent::getSerial)
        );

        private synchronized void addEvent(NetworkEvent event) {
            if (event.getSerial() == nextExpectedSerial) {
                downstream.accept(new ConnectionEvent(event, this));
                if (isOpen) {
                    nextExpectedSerial++;
                    processPending();
                }
            } else if (event.getSerial() > nextExpectedSerial) {
                pendingEvents.offer(event);
            } else {
                System.out.println(event.getConnectionId() + ": duplicate serial = " + event.getSerial());
            }
        }

        private synchronized void processPending() {
            NetworkEvent next;
            while ((next = pendingEvents.peek()) != null && next.getSerial() == nextExpectedSerial) {
                pendingEvents.poll();
                downstream.accept(new ConnectionEvent(next, this));
                nextExpectedSerial++;
            }
        }

        // OPEN is blocking operation so DATA must wait until it is complete
        // TODO add backpressure for DATA as well? How would it look like...
        public synchronized void nowOpen() {
            if (isOpen)
                System.err.println("Now open but already open");
            isOpen = true;
            nextExpectedSerial++;
            processPending();
        }
    }
    
    //public void shutdown() {
    //    executor.shutdown();
    //}
}