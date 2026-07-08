package pvt.muxalma.h2;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Muxalma;

public class H2FemaleMain {
    public static void main(String[] args) throws Exception {
        Lifecycle lifecycle = new Lifecycle();
        H2Retrieval retrieval = new H2Retrieval("build/db", "male_storage",
                Muxalma.female(18080, new H2Storage("build/db", "female_storage"), lifecycle));

        retrieval.start();

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy is running on port 18080");
        System.out.println("Supports both CONNECT (HTTPS) and regular HTTP");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            retrieval.stop();
            lifecycle.stop();
            System.out.println("Proxy stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }
}
