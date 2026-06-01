package pvt.muxalma.model;

import java.util.UUID;

public interface NetworkEvent {
    UUID getConnectionId();

    int getSerial();

    EventType getType();

    byte[] getPayload();

    static NetworkEvent create(UUID connectionId, int serial, EventType type, byte[] payload) {
        return new ConcreteEvent(connectionId, serial, type, payload);
    }
}
