package pvt.muxalma.h2;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import pvt.muxalma.fanservice.PollingRetrievalSupport;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class H2Retrieval extends PollingRetrievalSupport {
    private final String connectionUrl;
    private final String tableName;
    private final Consumer<NetworkEvent> targetConsumer;
    private final AtomicLong lastProcessedId;

    public H2Retrieval(String databasePath, String tableName, Consumer<NetworkEvent> targetConsumer) {
        this(databasePath, tableName, targetConsumer, 10);
    }

    public H2Retrieval(String databasePath, String tableName, Consumer<NetworkEvent> targetConsumer, long pollIntervalMs) {
        super(pollIntervalMs);
        this.connectionUrl = String.format(
                "jdbc:h2:%s;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000",
                Paths.get(databasePath).toAbsolutePath()
        );
        this.tableName = tableName;
        this.targetConsumer = targetConsumer;
        this.lastProcessedId = new AtomicLong(getMaxId());
    }

    private long getMaxId() {
        String query = String.format("SELECT COALESCE(MAX(id), 0) FROM %s", tableName);
        try (Connection conn = DriverManager.getConnection(connectionUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            return rs.next() ? rs.getLong(1) : 0;
        } catch (SQLException e) {
            return 0;
        }
    }

    protected void poll() {
        String selectSQL = String.format("""
                SELECT id, connection_id, serial, event_type, payload 
                FROM %s 
                WHERE id > ? 
                ORDER BY id ASC
                """, tableName);

        try (Connection conn = DriverManager.getConnection(connectionUrl);
             PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {

            pstmt.setLong(1, lastProcessedId.get());

            long eventId = lastProcessedId.get();
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    eventId = rs.getLong("id");
                    NetworkEvent event = extractEvent(rs);

                    targetConsumer.accept(event);

                }
            } finally {
                long finalEventId = eventId;
                lastProcessedId.updateAndGet(val -> Math.max(val, finalEventId));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to poll events", e);
        }
    }

    private NetworkEvent extractEvent(ResultSet rs) throws SQLException {
        UUID connectionId = (UUID) rs.getObject("connection_id");
        int serial = rs.getInt("serial");
        EventType type = EventType.valueOf(rs.getString("event_type"));
        byte[] payload = rs.getBytes("payload");

        return NetworkEvent.create(connectionId, serial, type, payload);
    }
}
