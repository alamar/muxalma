package pvt.muxalma.h2;

import pvt.muxalma.feminine.ConnectionManager;
import pvt.muxalma.feminine.HttpProxyServer;

public class H2FemaleMain {
    public static void main(String[] args) throws Exception {
        // Создаем менеджер соединений
        ConnectionManager connectionManager = new ConnectionManager();

        H2Retrieval retrieval = new H2Retrieval("build/db", "male_storage", connectionManager);

        H2Storage storage = new H2Storage("build/db", "female_storage");

        // Запускаем прокси-сервер
        HttpProxyServer server = new HttpProxyServer(18080, storage, connectionManager);
        server.start();
        retrieval.start();

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy is running on port 18080");
        System.out.println("Supports both CONNECT (HTTPS) and regular HTTP");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            retrieval.stop();
            server.stop();
            System.out.println("Proxy stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }
}
