package pvt.muxalma.processor;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.ConcreteEvent;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public abstract class ProcessorPushbackSupport implements Consumer<NetworkEvent> {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Consumer<NetworkEvent> pushback;

    protected ProcessorPushbackSupport(Consumer<NetworkEvent> pushback) {
        this.pushback = pushback;
    }

    protected void reportError(UUID connectionId, String msg) {
        log.warn("{}: {}", connectionId, msg);
        pushback.accept(new ConcreteEvent(connectionId, 0, EventType.ABORT, msg.getBytes(StandardCharsets.UTF_8)));
    }
}
