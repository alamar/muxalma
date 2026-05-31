package pvt.muxalma.model;

import java.util.UUID;

public interface NetworkEvent {
    UUID getConnectionId();

    int getSerial();

    EventType getType();

    byte[] getPayload();
}
