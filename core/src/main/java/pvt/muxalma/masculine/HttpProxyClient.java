package pvt.muxalma.masculine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class HttpProxyClient implements Consumer<NetworkEvent> {
    private final Consumer<NetworkEvent> upstreamConsumer;
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();
    private final ConcurrentHashMap<UUID, ClientConnection> connections = new ConcurrentHashMap<>();
    
    public HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        this.upstreamConsumer = upstreamConsumer;
    }
    
    @Override
    public void accept(NetworkEvent event) {
        switch (event.getEventType()) {
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
    
    private void handleOpen(NetworkEvent event) {
        String hostPort = event.getHostPort();
        String[] parts = hostPort.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 80;
        
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        new HttpClientCodec(),
                        new HttpObjectAggregator(65536),
                        new RemoteServerHandler(event.getConnectionId(), upstreamConsumer)
                    );
                }
            });
        
        ChannelFuture future = bootstrap.connect(host, port);
        ClientConnection conn = new ClientConnection();
        conn.setPendingEvent(event);
        connections.put(event.getConnectionId(), conn);
        
        future.addListener((ChannelFutureListener) f -> {
            if (f.isSuccess()) {
                conn.setChannel(f.channel());
                // Отправляем подтверждение, что OPEN успешен
                upstreamConsumer.accept(new NetworkEvent(
                    event.getConnectionId(),
                    -1, // специальный serial для ACK
                    EventType.DATA,
                    "CONNECTED".getBytes()
                ));
            } else {
                System.err.println("Failed to connect to " + hostPort);
                connections.remove(event.getConnectionId());
                upstreamConsumer.accept(new NetworkEvent(
                    event.getConnectionId(),
                    -1,
                    EventType.CLOSE,
                    null
                ));
            }
        });
    }
    
    private void handleData(NetworkEvent event) {
        ClientConnection conn = connections.get(event.getConnectionId());
        if (conn != null && conn.getChannel() != null && conn.getChannel().isActive()) {
            conn.getChannel().writeAndFlush(Unpooled.wrappedBuffer(event.getPayload()));
        }
    }
    
    private void handleClose(NetworkEvent event) {
        ClientConnection conn = connections.remove(event.getConnectionId());
        if (conn != null && conn.getChannel() != null) {
            conn.getChannel().close();
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
    
    private static class RemoteServerHandler extends SimpleChannelInboundHandler<HttpObject> {
        private final UUID connectionId;
        private final Consumer<NetworkEvent> upstreamConsumer;
        
        RemoteServerHandler(UUID connectionId, Consumer<NetworkEvent> upstreamConsumer) {
            this.connectionId = connectionId;
            this.upstreamConsumer = upstreamConsumer;
        }
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            if (msg instanceof FullHttpResponse) {
                FullHttpResponse response = (FullHttpResponse) msg;
                byte[] data = new byte[response.content().readableBytes()];
                response.content().readBytes(data);
                
                upstreamConsumer.accept(new NetworkEvent(
                    connectionId,
                    0, // serial можно управлять отдельно
                    EventType.DATA,
                    data
                ));
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            upstreamConsumer.accept(new NetworkEvent(
                connectionId,
                0,
                EventType.CLOSE,
                null
            ));
        }
    }
}