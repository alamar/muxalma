package pvt.muxalma.model;

import java.util.UUID;

public class ConcreteEvent implements NetworkEvent {
    private final UUID connectionId;
    private final int serial;
    private final EventType type;
    private final byte[] payload;
    
    public ConcreteEvent(UUID connectionId, int serial, EventType type, byte[] payload) {
        this.connectionId = connectionId;
        this.serial = serial;
        this.type = type;
        this.payload = payload != null ? payload.clone() : null;
    }
    
    @Override
    public UUID getConnectionId() { return connectionId; }
    @Override
    public int getSerial() { return serial; }
    @Override
    public EventType getType() { return type; }
    @Override
    public byte[] getPayload() { return payload != null ? payload.clone() : null; }
    
    @Override
    public String toString() {
        return String.format("ConcreteEvent{id=%s, serial=%d, type=%s, payloadLen=%d}",
            connectionId, serial, type, payload != null ? payload.length : 0);
    }
}
