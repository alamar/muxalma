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
import pvt.muxalma.model.ConcreteEvent;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class HttpProxyServer {
    private final int port;
    private final Consumer<NetworkEvent> eventConsumer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    // Для HTTP запросов (не CONNECT) нужно сразу соединяться с удаленным сервером
    private final EventLoopGroup clientWorkerGroup = new NioEventLoopGroup();
    private final ConcurrentHashMap<UUID, Channel> remoteChannels = new ConcurrentHashMap<>();

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
                                new ProxyServerHandler(eventConsumer, clientWorkerGroup, remoteChannels)
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
        clientWorkerGroup.shutdownGracefully();
    }

    private static class ProxyServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final Consumer<NetworkEvent> eventConsumer;
        private final EventLoopGroup clientWorkerGroup;
        private final ConcurrentHashMap<UUID, Channel> remoteChannels;

        private UUID connectionId;
        private final AtomicInteger serial = new AtomicInteger(0);
        private Channel remoteChannel;
        private boolean isConnect = false;

        ProxyServerHandler(Consumer<NetworkEvent> eventConsumer,
                           EventLoopGroup clientWorkerGroup,
                           ConcurrentHashMap<UUID, Channel> remoteChannels) {
            this.eventConsumer = eventConsumer;
            this.clientWorkerGroup = clientWorkerGroup;
            this.remoteChannels = remoteChannels;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
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

            System.out.println("CONNECT request to: " + uri);

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

            // Подготавливаемся к приему DATA от клиента
            ctx.pipeline().remove(HttpServerCodec.class);
            ctx.pipeline().remove(HttpObjectAggregator.class);
            ctx.pipeline().addLast(new TunnelHandler(ctx, connectionId, serial, eventConsumer, null));
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
                if (newUri.isEmpty()) newUri = "/";

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
                System.out.println("HTTP " + request.method() + " request to: " + host + ":" + port + newUri);

                // Отправляем OPEN событие
                String targetHostPort = host + ":" + port;
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.OPEN,
                        targetHostPort.getBytes()
                ));

                // Сериализуем модифицированный запрос в байты и отправляем как DATA
                byte[] requestData = serializeHttpRequest(modifiedRequest);
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        requestData
                ));

                // Для HTTP запросов, после ответа соединение закроется
                // Добавляем обработчик для получения ответа от удаленного сервера
                setupHttpResponseHandler(ctx);

            } catch (Exception e) {
                e.printStackTrace();
                sendError(ctx, HttpResponseStatus.BAD_REQUEST, "Invalid request: " + e.getMessage());
            }
        }

        private byte[] serializeHttpRequest(FullHttpRequest request) {
            // Простая сериализация HTTP запроса в байты
            StringBuilder sb = new StringBuilder();
            sb.append(request.method()).append(" ").append(request.uri()).append(" ")
                    .append(request.protocolVersion()).append("\r\n");

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

        private void setupHttpResponseHandler(ChannelHandlerContext ctx) {
            // Временно сохраняем remoteChannel для отправки ответа
            // Для HTTP мы ждем ответ от remote server через callback

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
        private final Channel remoteChannel;

        TunnelHandler(ChannelHandlerContext clientCtx, UUID connectionId,
                      AtomicInteger serial, Consumer<NetworkEvent> eventConsumer,
                      Channel remoteChannel) {
            this.clientCtx = clientCtx;
            this.connectionId = connectionId;
            this.serial = serial;
            this.eventConsumer = eventConsumer;
            this.remoteChannel = remoteChannel;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpContent) {
                byte[] data = new byte[((HttpContent) msg).content().readableBytes()];
                ((HttpContent) msg).content().readBytes(data);
                if (data.length > 0) {
                    eventConsumer.accept(new ConcreteEvent(
                            connectionId,
                            serial.getAndIncrement(),
                            EventType.DATA,
                            data
                    ));
                }
            } else if (msg instanceof byte[]) {
                eventConsumer.accept(new ConcreteEvent(
                        connectionId,
                        serial.getAndIncrement(),
                        EventType.DATA,
                        (byte[]) msg
                ));
            } else if (msg instanceof HttpRequest) {
                // Для HTTP запросов в туннеле (не должно быть)
                System.err.println("Unexpected HttpRequest in tunnel");
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            eventConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.CLOSE,
                    null
            ));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            eventConsumer.accept(new ConcreteEvent(
                    connectionId,
                    serial.getAndIncrement(),
                    EventType.ABORT,
                    (cause.getMessage() == null ? "ABORT" : cause.getMessage()).getBytes(StandardCharsets.UTF_8)
            ));
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
            cause.printStackTrace();
            clientCtx.close();
        }
    }
}
