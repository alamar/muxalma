package pvt.muxalma.masculine;

import java.util.UUID;

import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class ConnectionEvent implements NetworkEvent {
    private final NetworkEvent event;
    private final WaitingForOpenOrderingProcessor.ConnectionState state;

    public ConnectionEvent(NetworkEvent event, WaitingForOpenOrderingProcessor.ConnectionState state) {
        this.event = event;
        this.state = state;
    }

    @Override
    public UUID getConnectionId() {
        return event.getConnectionId();
    }

    @Override
    public int getSerial() {
        return event.getSerial();
    }

    @Override
    public EventType getType() {
        return event.getType();
    }

    @Override
    public byte[] getPayload() {
        return event.getPayload();
    }

    public WaitingForOpenOrderingProcessor.ConnectionState getState() {
        return state;
    }

    @Override
    public String toString() {
        return event.toString();
    }
}
