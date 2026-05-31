package pvt.muxalma.model;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

public class ValidationProcessor extends ProcessorPushbackSupport {
    private final Consumer<NetworkEvent> downstream;

    public ValidationProcessor(Consumer<NetworkEvent> downstream,
                               Consumer<NetworkEvent> pushback) {
        super(pushback);
        this.downstream = downstream;
    }
    
    @Override
    public void accept(NetworkEvent event) {
        if (event.getConnectionId() == null || event.getType() == null) {
            System.err.println("Invalid event: " + event);
            return;
        }

        if (event.getType() == EventType.ABORT) {
            // Everything is allowed, nothing to check
            System.err.println(event.getConnectionId() + ": " +
                    (event.getPayload() == null ? "ABORT" : new String(event.getPayload())));
            downstream.accept(event);
        }

        if (event.getSerial() < 0) {
            reportError(event.getConnectionId(), "Expecting serial >= 0, got " + event.getSerial());
            return;
        }

        if (event.getSerial() != 0 && event.getType() == EventType.OPEN) {
            reportError(event.getConnectionId(), "OPEN is expected to have serial = 0, got " + event.getSerial());
            return;
        }

        if ((event.getPayload() == null || event.getPayload().length == 0) &&
                (event.getType() == EventType.OPEN || event.getType() == EventType.DATA)) {
            reportError(event.getConnectionId(), "Expecting payload for " + event.getType());
            return;
        }

        downstream.accept(event);
    }
}
