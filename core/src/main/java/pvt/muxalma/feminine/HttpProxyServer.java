package pvt.muxalma.feminine;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class HttpProxyServer {
    private final int port;
    private final Consumer<NetworkEvent> eventConsumer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;
    
    public HttpProxyServer(int port, Consumer<NetworkEvent> eventConsumer) {
        this.port = port;
        this.eventConsumer = eventConsumer;
    }
    
    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(
                        new HttpServerCodec(),
                        new HttpObjectAggregator(65536),
                        new ProxyServerHandler(eventConsumer)
                    );
                }
            });
        
        channel = bootstrap.bind(port).sync().channel();
        System.out.println("HTTP Proxy Server started on port " + port);
    }
    
    public void stop() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
    
    private static class ProxyServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final Consumer<NetworkEvent> eventConsumer;
        private UUID connectionId;
        private final AtomicInteger serial = new AtomicInteger(0);
        private Channel remoteChannel; // Для CONNECT туннеля
        
        ProxyServerHandler(Consumer<NetworkEvent> eventConsumer) {
            this.eventConsumer = eventConsumer;
        }
        
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            if (request.method() == HttpMethod.CONNECT) {
                // HTTPS CONNECT прокси
                handleConnect(ctx, request);
            } else {
                // HTTP прокси (можно добавить позже)
                sendError(ctx, HttpResponseStatus.BAD_REQUEST);
            }
        }
        
        private void handleConnect(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            connectionId = UUID.randomUUID();
            
            // Отправляем OPEN событие с host:port
            eventConsumer.accept(new NetworkEvent(
                connectionId,
                serial.getAndIncrement(),
                EventType.OPEN,
                uri.getBytes()
            ));
            
            // Отвечаем клиенту, что соединение установлено
            HttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK
            );
            ctx.writeAndFlush(response);
            
            // Подготавливаемся к приему DATA от клиента
            ctx.pipeline().remove(HttpServerCodec.class);
            ctx.pipeline().remove(HttpObjectAggregator.class);
            ctx.pipeline().addLast(new TunnelHandler(ctx, connectionId, serial, eventConsumer));
        }
        
        private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                status,
                Unpooled.wrappedBuffer(("Proxy Error: " + status).getBytes())
            );
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            if (connectionId != null) {
                eventConsumer.accept(new NetworkEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.CLOSE,
                    null
                ));
            }
            ctx.close();
        }
    }
    
    // Обработчик туннеля для пересылки данных между клиентом и удаленным сервером
    private static class TunnelHandler extends ChannelInboundHandlerAdapter {
        private final ChannelHandlerContext clientCtx;
        private final UUID connectionId;
        private final AtomicInteger serial;
        private final Consumer<NetworkEvent> eventConsumer;
        
        TunnelHandler(ChannelHandlerContext clientCtx, UUID connectionId, 
                      AtomicInteger serial, Consumer<NetworkEvent> eventConsumer) {
            this.clientCtx = clientCtx;
            this.connectionId = connectionId;
            this.serial = serial;
            this.eventConsumer = eventConsumer;
        }
        
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent) {
                // Для простоты считаем, что после CONNECT идут только данные
                byte[] data = ((HttpContent) msg).content().array();
                if (data.length > 0) {
                    eventConsumer.accept(new NetworkEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        data
                    ));
                }
            } else if (msg instanceof byte[]) {
                eventConsumer.accept(new NetworkEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.DATA,
                    (byte[]) msg
                ));
            }
        }
        
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            eventConsumer.accept(new NetworkEvent(
                connectionId,
                serial.getAndIncrement(),
                EventType.CLOSE,
                null
            ));
        }
        
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            eventConsumer.accept(new NetworkEvent(
                connectionId,
                serial.getAndIncrement(),
                EventType.CLOSE,
                null
            ));
            ctx.close();
        }
    }
}