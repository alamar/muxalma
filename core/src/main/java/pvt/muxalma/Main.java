package pvt.muxalma;

import java.util.function.Consumer;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Loopback;
import pvt.muxalma.fanservice.Muxalma;
import pvt.muxalma.model.NetworkEvent;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Lifecycle lifecycle = new Lifecycle();

        // Создаем петлю
        Loopback loopback = new Loopback();

        // Создаем прокси-клиента "папа", которому пока некому отвечать
        Consumer<NetworkEvent> proxyClient = Muxalma.male(loopback, lifecycle);

        // Запускаем прокси-сервер "мама", и теперь они могут общаться
        loopback.initialize(Muxalma.female(18080, proxyClient, lifecycle));

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy is running on port 8080");
        System.out.println("Supports both CONNECT (HTTPS) and regular HTTP");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            lifecycle.stop();
            System.out.println("Proxy stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }
}
