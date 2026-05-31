package pvt.muxalma;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import pvt.muxalma.feminine.ConnectionManager;
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
        // Создаем менеджер соединений
        ConnectionManager connectionManager = new ConnectionManager();

        // Обработчик ответов от клиентской части (папа)
        // Сюда приходят ответы от удаленного сервера
        Consumer<NetworkEvent> clientResponseHandler = event -> {
            System.out.println("Response from client: " + event.getType() +
                    " for " + event.getConnectionId());

            // Передаем ответ в менеджер, который отправит его клиенту
            connectionManager.onResponseReceived(event);
        };

        // Создаем прокси клиента
        HttpProxyClient proxyClient = new HttpProxyClient(clientResponseHandler);

        // Обработчик событий от серверной части (мама)
        // Сюда приходят запросы от клиента браузера
        Consumer<ConnectionEvent> serverRequestHandler = event -> {
            System.out.println("Request from server: " + event.getType() +
                    " for " + event.getConnectionId());

            // Передаем событие прокси клиенту для обработки
            proxyClient.accept(event);
        };


        // Создаём цепочку для обработки событий
        OrderingProcessor ordered = new OrderingProcessor(serverRequestHandler);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(
                parallel, clientResponseHandler);

        // Запускаем прокси-сервер
        HttpProxyServer server = new HttpProxyServer(8080, valid, connectionManager);
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
