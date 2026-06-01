package pvt.muxalma;

import java.util.function.Consumer;

import pvt.muxalma.feminine.ConnectionManager;
import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.masculine.OrderingProcessor;
import pvt.muxalma.masculine.ParallelProcessor;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.model.ValidationProcessor;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // Создаем менеджер соединений
        ConnectionManager connectionManager = new ConnectionManager();

        // Создаем прокси клиента
        HttpProxyClient proxyClient = new HttpProxyClient(connectionManager::onResponseReceived);

        // Создаём цепочку для обработки событий
        OrderingProcessor ordered = new OrderingProcessor(proxyClient::accept);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(
                parallel, connectionManager::onResponseReceived);

        // Запускаем прокси-сервер
        HttpProxyServer server = new HttpProxyServer(18080, valid, connectionManager);
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
