package pvt.muxalma;

import java.util.function.Consumer;

import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.proxy.EventLoopProcessor;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // Создаем цепочку: сервер -> eventProcessor -> клиент -> eventProcessor -> сервер

        // Клиент отправляет ответы обратно серверу
        Consumer<NetworkEvent> clientToServer = event -> {
            System.out.println("Client -> Server: " + event);
            // Здесь будет логика отправки ответа клиенту через сервер
        };

        // Прокси клиент
        HttpProxyClient proxyClient = new HttpProxyClient(clientToServer);

        // Event процессор для упорядочивания событий от сервера
        EventLoopProcessor serverProcessor = new EventLoopProcessor(proxyClient);

        // Запускаем HTTP прокси сервер
        HttpProxyServer server = new HttpProxyServer(8080, serverProcessor);
        server.start();

        System.out.println("Proxy is running on http://localhost:8080");
        System.out.println("Configure your browser to use this proxy");

        // Ждем завершения (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop();
            proxyClient.shutdown();
        }));
    }
}
