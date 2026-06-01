package pvt.muxalma.h2;

import java.util.function.Consumer;

import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.masculine.OrderingProcessor;
import pvt.muxalma.masculine.ParallelProcessor;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.model.ValidationProcessor;

public class H2MaleMain {
    public static void main(String[] args) throws Exception {
        H2Storage storage = new H2Storage("build/db", "male_storage");

        // Создаем прокси клиента
        HttpProxyClient proxyClient = new HttpProxyClient(storage);

        // Создаём цепочку для обработки событий
        OrderingProcessor ordered = new OrderingProcessor(proxyClient);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(
                parallel, storage);

        H2Retrieval retrieval = new H2Retrieval("build/db", "female_storage", valid);

        retrieval.start();

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy relay is running...");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            retrieval.stop();
            // TODO stop client
            System.out.println("Proxy stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }
}
