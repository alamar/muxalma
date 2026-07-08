package pvt.muxalma.h2;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Muxalma;
import pvt.muxalma.masculine.HttpProxyClient;

public class H2MaleMain {
    public static void main(String[] args) throws Exception {
        Lifecycle lifecycle = new Lifecycle();
        final H2Storage maleStorage = new H2Storage("build/db", "male_storage");
        H2Retrieval retrieval = new H2Retrieval("build/db", "female_storage",
                Muxalma.male(new HttpProxyClient(maleStorage), lifecycle));

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
