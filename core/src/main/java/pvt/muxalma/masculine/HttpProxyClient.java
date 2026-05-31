package pvt.muxalma.masculine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import pvt.muxalma.model.ConcreteEvent;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class HttpProxyClient implements Consumer<ConnectionEvent> {
    private final Consumer<NetworkEvent> upstreamConsumer;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ConcurrentHashMap<UUID, ClientConnection> connections = new ConcurrentHashMap<>();

    public HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        this.upstreamConsumer = upstreamConsumer;
    }

    @Override
    public void accept(ConnectionEvent event) {
        System.err.println(event);
        switch (event.getType()) {
            case OPEN:
                handleOpen(event);
                break;
            case DATA:
                handleData(event);
                break;
            case CLOSE:
                handleClose(event);
                break;
        }
    }

    private void handleOpen(ConnectionEvent event) {
        String hostPort = new String(event.getPayload());
        String[] parts = hostPort.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 80;

        System.out.println("Client opening connection to: " + host + ":" + port);

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
                System.out.println("Connected to remote server: " + host + ":" + port);
                event.getState().nowOpen();
            } else {
                System.err.println("Failed to connect to " + hostPort + ": " + f.cause());
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
                System.out.println("Sent " + payload.length + " bytes to remote server");
            }
        } else {
            System.err.println("No active connection for DATA event: " + event.getConnectionId());
        }
    }

    private void handleClose(NetworkEvent event) {
        ClientConnection conn = connections.remove(event.getConnectionId());
        if (conn != null && conn.getChannel() != null) {
            conn.getChannel().close();
            System.out.println("Closed connection: " + event.getConnectionId());
        }
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

        RemoteServerHandler(UUID connectionId, Consumer<NetworkEvent> upstreamConsumer) {
            this.connectionId = connectionId;
            this.upstreamConsumer = upstreamConsumer;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf) {
                ByteBuf buffer = (ByteBuf) msg;
                byte[] data = new byte[buffer.readableBytes()];
                buffer.readBytes(data);

                // Отправляем ответ обратно в прокси
                upstreamConsumer.accept(new ConcreteEvent(
                        connectionId,
                        0, // serial можно игнорировать для ответов или сделать отдельный счетчик
                        EventType.DATA,
                        data
                ));

                System.out.println("Received " + data.length + " bytes from remote server");
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
                        0,
                        EventType.DATA,
                        fullResponse.getBytes(StandardCharsets.UTF_8)
                ));
            } else {
                System.err.println("Unknown message type: " + msg.getClass());
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            System.out.println("Remote server connection closed: " + connectionId);
            upstreamConsumer.accept(new ConcreteEvent(
                    connectionId,
                    0,
                    EventType.CLOSE,
                    null
            ));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("Error in remote connection: " + cause.getMessage());
            cause.printStackTrace();
            upstreamConsumer.accept(new ConcreteEvent(
                    connectionId,
                    0,
                    EventType.CLOSE,
                    null
            ));
            ctx.close();
        }
    }
}
