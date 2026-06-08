package pvt.muxalma.fanservice.harem;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import pvt.muxalma.model.NetworkEvent;

/**
 * Creates and keeps a storage for every client using provided generator.
 */
public class PrinceMultiStorage extends PrinceStorage {
    public final ConcurrentHashMap<UUID, Consumer<NetworkEvent>> memoizedStorage = new ConcurrentHashMap<>();
    public final Function<UUID, Consumer<NetworkEvent>> storageGenerator;

    public PrinceMultiStorage(Harem harem, Function<UUID, Consumer<NetworkEvent>> storageGenerator) {
        super(harem);
        this.storageGenerator = storageGenerator;
    }

    @Override
    protected void princeStores(UUID clientId, NetworkEvent event) {
        memoizedStorage.computeIfAbsent(clientId, storageGenerator).accept(event);
    }
}
