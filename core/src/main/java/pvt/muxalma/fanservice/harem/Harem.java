package pvt.muxalma.fanservice.harem;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Muxalma;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

/**
 * Tracks mapping of connection to client on male side.
 */
public class Harem {
    private static final Logger log = LoggerFactory.getLogger(Harem.class);

    private final ConcurrentHashMap<UUID, UUID> connectionIdToClientIdMap = new ConcurrentHashMap<>();

    public UUID clientIdFor(UUID connectionId) {
        return connectionIdToClientIdMap.get(connectionId);
    }

    // Сообщение от принца наложнице:
    // TODO сделать фильтром, а не странным методом? API поменяется
    public void respond(NetworkEvent event, BiConsumer<UUID, NetworkEvent> idAwareUpstream) {
        UUID clientId;

        if (event.getType() == EventType.CLOSE) {
            clientId = connectionIdToClientIdMap.remove(event.getConnectionId());
            if (clientId == null) {
                // Другая сторона уже закрыла соединение, нет необходимости отправить CLOSE
                // TODO: Также нужно подавлять сообщение на наложницах и в режиме мама-папа
                return;
            }
        } else {
            clientId = connectionIdToClientIdMap.get(event.getConnectionId());
        }

        if (clientId == null) {
            log.warn("No client expects connection {}", event.getConnectionId());
            return;
        }

        idAwareUpstream.accept(clientId, event);
    }

    public BiConsumer<UUID, NetworkEvent> prince(Consumer<NetworkEvent> storage, Lifecycle lifecycle) {
        Consumer<NetworkEvent> prince = Muxalma.male(storage, lifecycle);

        // Сообщение от наложницы принцу:
        return (clientId, event) -> {
            if (event.getType() == EventType.CLOSE) {
                connectionIdToClientIdMap.remove(event.getConnectionId());
            } else {
                // TODO handle collisions?
                connectionIdToClientIdMap.putIfAbsent(event.getConnectionId(), clientId);
            }

            prince.accept(event);
        };
    }
}
