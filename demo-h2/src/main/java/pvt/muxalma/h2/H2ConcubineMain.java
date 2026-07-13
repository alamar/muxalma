package pvt.muxalma.h2;

import java.util.UUID;

import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.harem.Concubine;

public class H2ConcubineMain {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: H2ConcubineMain {port}");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        Lifecycle lifecycle = new Lifecycle();
        Concubine myself = new Concubine() {
            // Ephemeral ID to allow running multiple instances in parallel
            @Override
            protected UUID loadOrGenerateClientId() {
                return UUID.randomUUID();
            }
        };

        H2ConcubineRetrieval retrieval = new H2ConcubineRetrieval("build/db", "prince_storage", myself.getClientId(),
                myself.female(port, new H2Storage("build/db", "concubines_storage"), lifecycle));

        retrieval.start();

        System.out.println("========================================");
        System.out.printf("HTTP Filtering Proxy is running on port %d%n", port);
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
