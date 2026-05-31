package pvt.muxalma.model;

import java.util.UUID;

public class NetworkEvent {
    private final UUID connectionId;
    private final int serial;
    private final EventType eventType;
    private final byte[] payload;
    
    public NetworkEvent(UUID connectionId, int serial, EventType eventType, byte[] payload) {
        this.connectionId = connectionId;
        this.serial = serial;
        this.eventType = eventType;
        this.payload = payload != null ? payload.clone() : null;
    }
    
    public UUID getConnectionId() { return connectionId; }
    public int getSerial() { return serial; }
    public EventType getEventType() { return eventType; }
    public byte[] getPayload() { return payload != null ? payload.clone() : null; }
    
    // Для OPEN события
    public String getHostPort() {
        if (eventType == EventType.OPEN && payload != null) {
            return new String(payload);
        }
        throw new IllegalStateException("Not an OPEN event");
    }
    
    @Override
    public String toString() {
        return String.format("NetworkEvent{id=%s, serial=%d, type=%s, payloadLen=%d}",
            connectionId, serial, eventType, payload != null ? payload.length : 0);
    }
}