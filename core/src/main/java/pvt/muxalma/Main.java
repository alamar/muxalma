package pvt.muxalma;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.masculine.ConnectionEvent;
import pvt.muxalma.masculine.OrderingProcessor;
import pvt.muxalma.masculine.ParallelProcessor;
import pvt.muxalma.model.ValidationProcessor;
import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.model.EventType;
import pvt.muxalma.model.NetworkEvent;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // Хранилище для каналов, чтобы отправлять ответы обратно клиентам
        ConcurrentHashMap<UUID, ResponseHandler> responseHandlers = new ConcurrentHashMap<>();

        // Обработчик ответов от клиента (папа) к серверу (мама)
        Consumer<NetworkEvent> clientToServer = event -> {
            System.out.println("Response from client: " + event);

            ResponseHandler handler = responseHandlers.get(event.getConnectionId());
            if (handler != null && event.getType() == EventType.DATA) {
                handler.onResponse(event.getPayload());
            } else if (event.getType() == EventType.CLOSE) {
                ResponseHandler closed = responseHandlers.remove(event.getConnectionId());
                if (closed != null) {
                    closed.onClose();
                }
            }
        };

        // Создаем прокси клиент
        HttpProxyClient proxyClient = new HttpProxyClient(clientToServer);

        // Обработчик событий от сервера (мама) к клиенту (папа)
        Consumer<ConnectionEvent> serverToClient = event -> {
            System.out.println("Request from server: " + event);

            if (event.getType() == EventType.OPEN) {
                // Создаем обработчик для этого соединения
                ResponseHandler handler = new ResponseHandler() {
                    @Override
                    public void onResponse(byte[] data) {
                        // Отправляем ответ обратно в серверную часть
                        // Здесь нужно будет вызвать callback на сервере
                        System.out.println("Would send response to server");
                    }

                    @Override
                    public void onClose() {
                        System.out.println("Connection closed");
                    }
                };
                responseHandlers.put(event.getConnectionId(), handler);
            }

            // Передаем событие клиенту для обработки
            proxyClient.accept(event);
        };

        // Создаём цепочку для обработки событий
        OrderingProcessor ordered = new OrderingProcessor(serverToClient);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(
                parallel, clientToServer);

        // Запускаем прокси-сервер
        HttpProxyServer server = new HttpProxyServer(8080, valid);
        server.start();

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy is running on port 8080");
        System.out.println("Supports both CONNECT (HTTPS) and regular HTTP");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            server.stop();
            proxyClient.shutdown();
            parallel.shutdown();
            System.out.println("Proxy stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }

    interface ResponseHandler {
        void onResponse(byte[] data);
        void onClose();
    }
}
