package pvt.muxalma.feminine;

import io.netty.channel.Channel;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionManager {
    private static Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    // Маппинг connectionId -> Netty channel клиента (кто запросил)
    private final ConcurrentHashMap<UUID, Channel> clientChannels = new ConcurrentHashMap<>();
    
    // Для обратной связи от клиентской части к серверной
    private final ConcurrentHashMap<UUID, ResponseCallback> pendingResponses = new ConcurrentHashMap<>();
    
    public void registerClientChannel(UUID connectionId, Channel channel) {
        clientChannels.put(connectionId, channel);
        if (log.isDebugEnabled()) {
            log.debug("Registered channel for connection: {}", connectionId);
        }
    }
    
    public void unregisterClientChannel(UUID connectionId) {
        clientChannels.remove(connectionId);
        pendingResponses.remove(connectionId);
        if (log.isDebugEnabled()) {
            log.debug("Unregistered channel for connection: {}", connectionId);
        }
    }
    
    public void sendToClient(UUID connectionId, byte[] data) {
        Channel channel = clientChannels.get(connectionId);
        try {
            if (channel != null && channel.isActive()) {
                channel.writeAndFlush(Unpooled.wrappedBuffer(data)).sync();
                if (log.isDebugEnabled()) {
                    log.debug("Sent {} bytes to client for connection: {}", data.length, connectionId);
                }
            } else {
                log.info("No active channel for connection: {}", connectionId);
            }
        } catch (Throwable e) {
            log.warn("While sending data to {}", connectionId, e);
        }
    }
    
    public void registerResponseCallback(UUID connectionId, ResponseCallback callback) {
        pendingResponses.put(connectionId, callback);
    }
    
    public void onResponseReceived(NetworkEvent event) {
        ResponseCallback callback = pendingResponses.get(event.getConnectionId());
        if (callback != null) {
            callback.onResponse(event);
        } else {
            // Если нет callback, значит это ответ для обычного HTTP запроса
            // Отправляем напрямую клиенту
            if (event.getType() == EventType.DATA) {
                sendToClient(event.getConnectionId(), event.getPayload());
            } else if (event.getType() == EventType.CLOSE || event.getType() == EventType.ABORT) {
                sendToClient(event.getConnectionId(), new byte[0]);
                Channel channel = clientChannels.get(event.getConnectionId());
                if (channel != null) {
                    channel.close();
                }
                unregisterClientChannel(event.getConnectionId());
            }
        }
    }
    
    public interface ResponseCallback {
        void onResponse(NetworkEvent event);
    }
}