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
 * Remembers per-PC client ID, does all storages in context of this ID.
 */
public class Concubine {
    private final UUID clientId;

    public Concubine() throws IOException {
        this.clientId = loadOrGenerateClientId();
    }

    public UUID getClientId() {
        return clientId;
    }

    public Consumer<NetworkEvent> storage(BiConsumer<UUID, NetworkEvent> idAwareStorage) {
        return event -> idAwareStorage.accept(clientId, event);
    }

    private UUID loadOrGenerateClientId() throws IOException {
        Path configFile = getConfigFilePath();

        // Ensure parent directory exists
        Path parentDir = configFile.getParent();
        if (parentDir != null && !Files.exists(parentDir)) {
            Files.createDirectories(parentDir);
        }

        if (Files.exists(configFile)) {
            // Read existing UUID
            String uuidString = Files.readString(configFile).trim();
            try {
                return UUID.fromString(uuidString);
            } catch (IllegalArgumentException e) {
                throw new IOException("Invalid UUID format in config file: " + configFile, e);
            }
        } else {
            // Generate new UUID
            UUID newUuid = UUID.randomUUID();
            String uuidString = newUuid.toString();

            try {
                // Potential race condition irrelevant as it should not be run in parallel at all, but guard against overwriting.
                Files.writeString(configFile, uuidString,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE_NEW);
            } catch (IOException e) {
                throw new IOException("Failed to write UUID to config file: " + configFile, e);
            }

            return newUuid;
        }
    }

    private Path getConfigFilePath() {
        String osName = System.getProperty("os.name").toLowerCase();
        Path configDir;

        if (osName.contains("win")) {
            // Windows: %APPDATA%\muxalma
            String appData = System.getenv("APPDATA");
            if (appData == null) {
                // Fallback to user.home if APPDATA is not set
                appData = System.getProperty("user.home");
                configDir = Paths.get(appData, "AppData", "Roaming", "muxalma");
            } else {
                configDir = Paths.get(appData, "muxalma");
            }
        } else {
            // Unix-like systems (Linux, macOS): ~/.config/muxalma
            String userHome = System.getProperty("user.home");
            configDir = Paths.get(userHome, ".config", "muxalma");
        }

        return configDir.resolve("client.id");
    }

    public BiConsumer<UUID, NetworkEvent> female(int port, BiConsumer<UUID, NetworkEvent> idAwareStorage, Lifecycle lifecycle) throws InterruptedException {
        Consumer<NetworkEvent> myself = Muxalma.female(port, this.storage(idAwareStorage), lifecycle);
        return (uuid, event) -> myself.accept(event);
    }
}
