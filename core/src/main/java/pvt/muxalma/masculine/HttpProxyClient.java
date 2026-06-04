package pvt.muxalma.masculine;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.ConcreteEvent;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class HttpProxyClient implements Consumer<ConnectionEvent> {
    private static final Logger log = LoggerFactory.getLogger(HttpProxyClient.class);

    private final Consumer<NetworkEvent> upstreamConsumer;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ConcurrentHashMap<UUID, ClientConnection> connections = new ConcurrentHashMap<>();

    public HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        this.upstreamConsumer = upstreamConsumer;
    }

    @Override
    public void accept(ConnectionEvent event) {
        switch (event.getType()) {
            case OPEN:
                handleOpen(event);
                break;
            case DATA:
                handleData(event);
                break;
            case CLOSE:
            case ABORT:
                handleClose(event);
                break;
        }
    }

    private void handleOpen(ConnectionEvent event) {
        String hostPort = new String(event.getPayload());
        String[] parts = hostPort.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 80;

        if (log.isDebugEnabled()) {
            log.debug("Client opening connection to: {}:{}", host, port);
        }

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new RemoteServerHandler(event.getConnectionId(), upstreamConsumer));
                    }
                });

        ClientConnection conn = new ClientConnection();
        conn.setPendingEvent(event);
        connections.put(event.getConnectionId(), conn);

        ChannelFuture future = bootstrap.connect(host, port);
        future.addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                conn.setChannel(f.channel());
                if (log.isDebugEnabled()) {
                    log.debug("Connected to remote server: {}:{}", host, port);
                }
                event.getState().nowOpen();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to connect to {}", hostPort, f.cause());
                }
                connections.remove(event.getConnectionId()); // TODO notify OrderingProcessor to drop events
                upstreamConsumer.accept(new ConcreteEvent(
                        event.getConnectionId(),
                        -1,
                        EventType.ABORT,
                        // TODO Pushback Processor?
                        ("Failed to connect to " + hostPort + ": " + f.cause()).getBytes(StandardCharsets.UTF_8)
                ));
            }
        });
    }

    private void handleData(NetworkEvent event) {
        ClientConnection conn = connections.get(event.getConnectionId());
        if (conn != null && conn.getChannel() != null && conn.getChannel().isActive()) {
            byte[] payload = event.getPayload();
            if (payload != null && payload.length > 0) {
                ByteBuf buffer = Unpooled.wrappedBuffer(payload);
                conn.getChannel().writeAndFlush(buffer);
                if (log.isDebugEnabled()) {
                    log.debug("Sent {} bytes to remote server", payload.length);
                }
            }
        } else {
            log.info("No active connection for DATA event: {}", event.getConnectionId());
        }
    }

    private void handleClose(NetworkEvent event) {
        ClientConnection conn = connections.remove(event.getConnectionId());
        if (conn != null && conn.getChannel() != null) {
            conn.getChannel().close();
            if (log.isDebugEnabled()) {
                log.debug("Closed connection: {}", event.getConnectionId());
            }
        }
        // Пробрасываем CLOSE дальше в серверную часть
        upstreamConsumer.accept(event);
    }

    public void shutdown() {
        workerGroup.shutdownGracefully();
    }

    private static class ClientConnection {
        private Channel channel;
        private NetworkEvent pendingEvent;

        public Channel getChannel() { return channel; }
        public void setChannel(Channel channel) { this.channel = channel; }
        public NetworkEvent getPendingEvent() { return pendingEvent; }
        public void setPendingEvent(NetworkEvent pendingEvent) { this.pendingEvent = pendingEvent; }
    }

    private static class RemoteServerHandler extends ChannelInboundHandlerAdapter {
        private final UUID connectionId;
        private final Consumer<NetworkEvent> upstreamConsumer;
        private final StringBuilder responseBuilder = new StringBuilder();
        private AtomicInteger serial = new AtomicInteger();

        RemoteServerHandler(UUID connectionId, Consumer<NetworkEvent> upstreamConsumer) {
            this.connectionId = connectionId;
            this.upstreamConsumer = upstreamConsumer;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf buffer) try {
                byte[] data = new byte[buffer.readableBytes()];
                buffer.readBytes(data);

                if (log.isDebugEnabled()) {
                    log.debug("Received {} bytes from remote server for {}", data.length, connectionId);
                }

                // Отправляем DATA обратно в серверную часть
                upstreamConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        data
                ));
            } finally {
                buffer.release();
            } else if (msg instanceof HttpResponse) {
                // Для HTTP ответов, парсим и отправляем
                // В реальном коде лучше использовать HttpObjectAggregator
                HttpResponse response = (HttpResponse) msg;
                responseBuilder.setLength(0);
                responseBuilder.append(response.protocolVersion()).append(" ")
                        .append(response.status()).append("\r\n");
                for (String name : response.headers().names()) {
                    responseBuilder.append(name).append(": ").append(response.headers().get(name)).append("\r\n");
                }
                responseBuilder.append("\r\n");
            } else if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                byte[] contentBytes = new byte[content.content().readableBytes()];
                content.content().readBytes(contentBytes);

                String fullResponse = responseBuilder.toString() + new String(contentBytes, StandardCharsets.UTF_8);
                upstreamConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        fullResponse.getBytes(StandardCharsets.UTF_8)
                ));
            } else {
                log.warn("Unknown message type: {}", msg.getClass());
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Remote server connection closed due to inactivity: {}", connectionId);
            // Отправляем CLOSE обратно
            upstreamConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.CLOSE,
                    null
            ));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Error in remote connection", cause);
            upstreamConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.ABORT,
                    null
            ));
            ctx.close();
        }
    }
}
