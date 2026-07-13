package pvt.muxalma.fanservice.harem;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import pvt.muxalma.model.NetworkEvent;

/**
 * Remembers client ID to pass it to a single storage instance.
 */
public class PrinceSingleStorage extends PrinceMultiStorage {
    public PrinceSingleStorage(Harem harem, BiConsumer<UUID, NetworkEvent> storage) {
        // Average Haskell Enjoyer:
        super(harem, uuid -> event -> storage.accept(uuid, event));
    }
}
