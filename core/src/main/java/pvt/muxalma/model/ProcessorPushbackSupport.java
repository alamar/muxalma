package pvt.muxalma.model;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.masculine.HttpProxyClient;

public abstract class ProcessorPushbackSupport implements Consumer<NetworkEvent> {
    protected Logger log = LoggerFactory.getLogger(getClass());

    protected final Consumer<NetworkEvent> pushback;

    protected ProcessorPushbackSupport(Consumer<NetworkEvent> pushback) {
        this.pushback = pushback;
    }

    protected void reportError(UUID connectionId, String msg) {
        log.warn("{}: {}", connectionId, msg);
        pushback.accept(new ConcreteEvent(connectionId, 0, EventType.ABORT, msg.getBytes(StandardCharsets.UTF_8)));
    }
}
