package pvt.muxalma.feminine;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pvt.muxalma.model.ConcreteEvent;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class HttpProxyServer {
    private static Logger log = LoggerFactory.getLogger(HttpProxyServer.class);

    private final int port;
    private final Consumer<NetworkEvent> eventConsumer;
    private final ConnectionManager connectionManager;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public HttpProxyServer(int port, Consumer<NetworkEvent> eventConsumer, ConnectionManager connectionManager) {
        this.port = port;
        this.eventConsumer = eventConsumer;
        this.connectionManager = connectionManager;
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
                                new ProxyServerHandler(eventConsumer, connectionManager)
                        );
                    }
                });

        channel = bootstrap.bind(port).sync().channel();
        log.info("HTTP Proxy Server started on port {}", port);
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
        private final ConnectionManager connectionManager;

        private UUID connectionId;
        private final AtomicInteger serial = new AtomicInteger(0);
        private boolean isConnect = false;
        private ChannelHandlerContext clientCtx;

        ProxyServerHandler(Consumer<NetworkEvent> eventConsumer, ConnectionManager connectionManager) {
            this.eventConsumer = eventConsumer;
            this.connectionManager = connectionManager;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            this.clientCtx = ctx;

            if (request.method() == HttpMethod.CONNECT) {
                // HTTPS CONNECT прокси
                handleConnect(ctx, request);
            } else {
                // Обычный HTTP запрос
                handleHttpRequest(ctx, request);
            }
        }

        private void handleConnect(ChannelHandlerContext ctx, FullHttpRequest request) {
            String uri = request.uri();
            connectionId = UUID.randomUUID();
            isConnect = true;

            if (log.isDebugEnabled()) {
                log.debug("CONNECT request to: {}", uri);
            }

            // Регистрируем канал клиента в менеджере
            connectionManager.registerClientChannel(connectionId, ctx.channel());

            // Отправляем OPEN событие с host:port
            eventConsumer.accept(new ConcreteEvent(
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

            setupTunnelHandler(ctx);
        }

        private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
            try {
                // Парсим URI для получения хоста и порта
                String uri = request.uri();
                String host;
                int port;

                // Убираем "http://" или "https://" если есть
                if (uri.startsWith("http://")) {
                    uri = uri.substring(7);
                } else if (uri.startsWith("https://")) {
                    uri = uri.substring(8);
                }

                // Находим host:port или просто host
                int slashIndex = uri.indexOf('/');
                String hostPort = slashIndex > 0 ? uri.substring(0, slashIndex) : uri;

                if (hostPort.contains(":")) {
                    String[] parts = hostPort.split(":");
                    host = parts[0];
                    port = Integer.parseInt(parts[1]);
                } else {
                    host = hostPort;
                    port = 80; // default HTTP port
                }

                // Модифицируем запрос - убираем схему и хост из URI
                String newUri = slashIndex > 0 ? uri.substring(slashIndex) : "/";

                // Создаем новый запрос с исправленным URI
                FullHttpRequest modifiedRequest = new DefaultFullHttpRequest(
                        request.protocolVersion(),
                        request.method(),
                        newUri,
                        request.content().copy(),
                        request.headers().copy(),
                        EmptyHttpHeaders.INSTANCE
                );

                // Добавляем Host заголовок если его нет
                if (!modifiedRequest.headers().contains(HttpHeaderNames.HOST)) {
                    modifiedRequest.headers().set(HttpHeaderNames.HOST, hostPort);
                }

                connectionId = UUID.randomUUID();

                // Регистрируем канал для ответа
                connectionManager.registerClientChannel(connectionId, ctx.channel());

                if (log.isDebugEnabled()) {
                    log.debug("HTTP {} request to: {}:{}{}", request.method(), host, port, newUri);
                }

                // Отправляем OPEN событие
                String targetHostPort = host + ":" + port;
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.OPEN,
                        targetHostPort.getBytes()
                ));

                // Сериализуем модифицированный запрос в байты и отправляем как DATA
                byte[] requestData = serializeHttpRequest(request, newUri);
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        requestData
                ));

                // Для HTTP запросов, после ответа соединение закроется
                // Добавляем обработчик для получения ответа от удаленного сервера
                connectionManager.registerResponseCallback(connectionId, event -> {
                    if (event.getType() == EventType.DATA) {
                        if (log.isDebugEnabled()) {
                            log.debug("Received response len = {}, sending to client {}",
                                    event.getPayload().length, connectionId);
                        }
                        connectionManager.sendToClient(connectionId, event.getPayload());
                    } else if (event.getType() == EventType.CLOSE || event.getType() == EventType.ABORT) {
                        if (log.isDebugEnabled()) {
                            log.debug("Remote connection closed, closing client connection {}", connectionId);
                        }
                        ctx.close();
                        connectionManager.unregisterClientChannel(connectionId);
                    }
                });

                setupHttpResponseHandler(ctx);
            } catch (Exception e) {
                log.warn("While handling http request to {}", request.uri(), e);
                sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid request: " + e.getMessage());
            }
        }

        private byte[] serializeHttpRequest(FullHttpRequest request, String newUri) {
            StringBuilder sb = new StringBuilder();
            sb.append(request.method()).append(" ").append(newUri).append(" ")
                    .append(request.protocolVersion()).append("\r\n");

            // Копируем все заголовки
            for (String name : request.headers().names()) {
                sb.append(name).append(": ").append(request.headers().get(name)).append("\r\n");
            }
            sb.append("\r\n");

            byte[] headerBytes = sb.toString().getBytes();
            byte[] contentBytes = new byte[request.content().readableBytes()];
            request.content().readBytes(contentBytes);

            byte[] result = new byte[headerBytes.length + contentBytes.length];
            System.arraycopy(headerBytes, 0, result, 0, headerBytes.length);
            System.arraycopy(contentBytes, 0, result, headerBytes.length, contentBytes.length);

            return result;
        }

        private void setupTunnelHandler(ChannelHandlerContext ctx) {
            // Подготавливаемся к приему DATA от клиента
            ctx.pipeline().remove(HttpServerCodec.class);
            ctx.pipeline().remove(HttpObjectAggregator.class);
            ctx.pipeline().addLast(new TunnelHandler(ctx, connectionId, serial, eventConsumer, connectionManager));
        }

        private void setupHttpResponseHandler(ChannelHandlerContext ctx) {
            // Добавляем обработчик для получения ответа от клиентской части
            ctx.pipeline().remove(HttpServerCodec.class);
            ctx.pipeline().remove(HttpObjectAggregator.class);
            ctx.pipeline().addLast(new HttpResponseHandler(ctx, connectionId));
        }

        private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status, String message) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    status,
                    Unpooled.wrappedBuffer(("Proxy Error: " + message).getBytes())
            );
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            if (connectionId != null) {
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.ABORT,
                        (cause.getMessage() == null ? "ABORT" : cause.getMessage()).getBytes(StandardCharsets.UTF_8)
                ));
                connectionManager.unregisterClientChannel(connectionId);
            }
            ctx.close();
        }
    }

    // Обработчик для CONNECT туннеля
    private static class TunnelHandler extends ChannelInboundHandlerAdapter {
        private final ChannelHandlerContext clientCtx;
        private final UUID connectionId;
        private final AtomicInteger serial;
        private final Consumer<NetworkEvent> eventConsumer;
        private final ConnectionManager connectionManager;

        TunnelHandler(ChannelHandlerContext clientCtx, UUID connectionId,
                      AtomicInteger serial, Consumer<NetworkEvent> eventConsumer,
                      ConnectionManager connectionManager) {
            this.clientCtx = clientCtx;
            this.connectionId = connectionId;
            this.serial = serial;
            this.eventConsumer = eventConsumer;
            this.connectionManager = connectionManager;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            byte[] data = null;

            if (msg instanceof HttpContent) {
                HttpContent content = (HttpContent) msg;
                data = new byte[content.content().readableBytes()];
                content.content().readBytes(data);
            } else if (msg instanceof ByteBuf) {
                ByteBuf buffer = (ByteBuf) msg;
                data = new byte[buffer.readableBytes()];
                buffer.readBytes(data);
            }

            if (data != null && data.length > 0) {
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        data
                ));
            } else if (msg instanceof HttpRequest) {
                // Для HTTP запросов в туннеле (не должно быть)
                log.warn("Unexpected HttpRequest in tunnel");
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("Client channel inactive for connection: {}", connectionId);
            eventConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.CLOSE,
                    null
            ));
            connectionManager.unregisterClientChannel(connectionId);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Exception caught", cause);
            eventConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.ABORT,
                    (cause.getMessage() == null ? "ABORT" : cause.getMessage()).getBytes(StandardCharsets.UTF_8)
            ));
            connectionManager.unregisterClientChannel(connectionId);
            ctx.close();
        }
    }

    // Обработчик для HTTP ответов
    private static class HttpResponseHandler extends SimpleChannelInboundHandler<byte[]> {
        private final ChannelHandlerContext clientCtx;
        private final UUID connectionId;

        HttpResponseHandler(ChannelHandlerContext clientCtx, UUID connectionId) {
            this.clientCtx = clientCtx;
            this.connectionId = connectionId;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, byte[] data) {
            // Получили ответ от удаленного сервера через event систему
            // Отправляем клиенту
            clientCtx.writeAndFlush(Unpooled.wrappedBuffer(data));
}

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // Remote channel closed, закрываем клиентское соединение
            clientCtx.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.warn("Exception caught", cause);
            clientCtx.close();
        }
    }
}
