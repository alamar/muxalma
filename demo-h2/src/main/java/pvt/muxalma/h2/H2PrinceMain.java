package pvt.muxalma.h2;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.harem.Harem;
import pvt.muxalma.fanservice.harem.PrinceSingleStorage;
import pvt.muxalma.masculine.HttpProxyClient;

public class H2PrinceMain {
    public static void main(String[] args) throws Exception {
        Lifecycle lifecycle = new Lifecycle();
        Harem harem = new Harem();
        H2PrinceRetrieval retrieval = new H2PrinceRetrieval("build/db", "concubines_storage",
                harem.prince(new HttpProxyClient(new PrinceSingleStorage(harem,
                        new H2Storage("build/db", "prince_storage"))), lifecycle));

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
