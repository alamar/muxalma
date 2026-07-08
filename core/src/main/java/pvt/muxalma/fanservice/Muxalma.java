package pvt.muxalma.fanservice;

import java.util.function.Consumer;

import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.feminine.SimpleOrderingProcessor;
import pvt.muxalma.masculine.WaitingForOpenOrderingProcessor;
import pvt.muxalma.processor.ParallelProcessor;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.processor.ValidationProcessor;

public class Muxalma {
    public static Consumer<NetworkEvent> male(HttpProxyClient proxyClient, Lifecycle lifecycle) {
        if (lifecycle != null)
            lifecycle.addStopMethod(proxyClient::shutdown);

        // Создаём цепочку для обработки событий
        WaitingForOpenOrderingProcessor ordered = new WaitingForOpenOrderingProcessor(proxyClient);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(parallel, proxyClient.getUpstreamConsumer());

        if (lifecycle != null)
            lifecycle.addStopMethod(parallel::shutdown);

        return valid;
    }

    public static Consumer<NetworkEvent> female(int port, Consumer<NetworkEvent> storage, Lifecycle lifecycle) throws InterruptedException {
        // Запускаем прокси-сервер "мама"
        HttpProxyServer server = new HttpProxyServer(port, storage);
        server.start();
        if (lifecycle != null)
            lifecycle.addStopMethod(server::stop);

        SimpleOrderingProcessor ordered = new SimpleOrderingProcessor(server);
        ParallelProcessor parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(parallel, storage);

        if (lifecycle != null)
            lifecycle.addStopMethod(parallel::shutdown);

        return valid;
    }
}
