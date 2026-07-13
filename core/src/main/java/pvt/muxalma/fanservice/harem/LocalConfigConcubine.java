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
import pvt.muxalma.model.NetworkEvent;

/**
 * Remembers per-PC client ID in a config file.
 */
public class LocalConfigConcubine extends Concubine {

    public LocalConfigConcubine() throws IOException {
        // No-op.
    }

    @Override
    protected UUID loadOrGenerateClientId() throws IOException {
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
}
