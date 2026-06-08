package pvt.muxalma.fanservice.harem;

import java.util.UUID;
import java.util.function.Consumer;

import pvt.muxalma.model.NetworkEvent;

/**
 * Does storages in context of already known client ID for a connection.
 */
public abstract class PrinceStorage implements Consumer<NetworkEvent> {
    private final Harem harem;

    public PrinceStorage(Harem harem) {
        this.harem = harem;
    }

    @Override
    public void accept(NetworkEvent event) {
        harem.respond(event, this::princeStores);
    }

    protected abstract void princeStores(UUID clientId, NetworkEvent event);
}
