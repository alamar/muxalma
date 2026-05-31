package pvt.muxalma.model;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

public abstract class ProcessorPushbackSupport implements Consumer<NetworkEvent> {
    protected final Consumer<NetworkEvent> pushback;

    protected ProcessorPushbackSupport(Consumer<NetworkEvent> pushback) {
        this.pushback = pushback;
    }

    protected void reportError(UUID connectionId, String msg) {
        System.err.println(connectionId + ": " + msg);
        pushback.accept(new ConcreteEvent(connectionId, 0, EventType.ABORT, msg.getBytes(StandardCharsets.UTF_8)));
    }
}
