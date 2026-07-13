package pvt.muxalma.fanservice.harem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Muxalma;
import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.feminine.SimpleOrderingProcessor;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.processor.ParallelProcessor;
import pvt.muxalma.processor.ValidationProcessor;

/**
 * Female side that does all storages in context of client ID.
 */
public abstract class Concubine {
    private final UUID clientId;

    public Concubine() throws IOException {
        this.clientId = loadOrGenerateClientId();
    }

    public UUID getClientId() {
        return clientId;
    }

    protected abstract UUID loadOrGenerateClientId() throws IOException;

    public Consumer<NetworkEvent> storage(BiConsumer<UUID, NetworkEvent> idAwareStorage) {
        return event -> idAwareStorage.accept(clientId, event);
    }

    public BiConsumer<UUID, NetworkEvent> female(int port, BiConsumer<UUID, NetworkEvent> idAwareStorage, Lifecycle lifecycle) throws InterruptedException {
        Consumer<NetworkEvent> myself = Muxalma.female(port, this.storage(idAwareStorage), lifecycle);
        return (uuid, event) -> myself.accept(event);
    }
}
