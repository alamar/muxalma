package pvt.muxalma.h2;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import pvt.muxalma.model.NetworkEvent;

public class H2Storage implements BiConsumer<UUID, NetworkEvent> {
    private final String connectionUrl;
    private final String tableName;

    public H2Storage(String databasePath, String tableName) {
        this.connectionUrl = String.format(
                "jdbc:h2:%s;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
                Paths.get(databasePath).toAbsolutePath()
        );
        this.tableName = tableName;
        initTable();
    }

    private void initTable() {
        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                client_id UUID NOT NULL,
                connection_id UUID NOT NULL,
                serial INT NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                payload BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """, tableName);

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize table", e);
        }
    }

    @Override
    public void accept(UUID clientId, NetworkEvent event) {
        String insertSQL = String.format("""
            INSERT INTO %s (client_id, connection_id, serial, event_type, payload) 
            VALUES (?, ?, ?, ?, ?)
            """, tableName);

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            pstmt.setObject(1, clientId);
            pstmt.setObject(2, event.getConnectionId());
            pstmt.setInt(3, event.getSerial());
            pstmt.setString(4, event.getType().name());
            pstmt.setBytes(5, event.getPayload());
            pstmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to store network event", e);
        }
    }
}
