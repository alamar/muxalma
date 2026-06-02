package pvt.muxalma.h2;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Muxalma;

public class H2MaleMain {
    public static void main(String[] args) throws Exception {
        Lifecycle lifecycle = new Lifecycle();
        H2Retrieval retrieval = new H2Retrieval("build/db", "female_storage",
                Muxalma.male(new H2Storage("build/db", "male_storage"), lifecycle));

        retrieval.start();

        System.out.println("========================================");
        System.out.println("HTTP Filtering Proxy relay is running...");
        System.out.println("========================================");

        // Добавляем обработчик завершения
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down proxy...");
            retrieval.stop();
            lifecycle.stop();
            System.out.println("Relay stopped");
        }));

        // Ждем бесконечно
        Thread.currentThread().join();
    }
}
