package pvt.muxalma;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pvt.muxalma.fanservice.Lifecycle;
import pvt.muxalma.fanservice.Loopback;
import pvt.muxalma.fanservice.Muxalma;
import pvt.muxalma.model.NetworkEvent;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProxyIntegrationTest {
    private Lifecycle lifecycle = new Lifecycle();

    @BeforeAll
    void setup() throws InterruptedException {
        // Создаем петлю
        Loopback loopback = new Loopback();

        // Создаем прокси-клиента "папа", которому пока некому отвечать
        Consumer<NetworkEvent> proxyClient = Muxalma.male(loopback, lifecycle);

        // Запускаем прокси-сервер "мама", и теперь они могут общаться
        loopback.initialize(Muxalma.female(18080, proxyClient, lifecycle));
        
        // Даем время на запуск
        Thread.sleep(1000);
    }
    
    @AfterAll
    void cleanup() {
        lifecycle.stop();
    }
    
    @Test
    void testSimpleHttpRequest() throws IOException {
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 18080));
        URL url = new URL("http://httpbin.org/get");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection(proxy);
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);
        
        int responseCode = conn.getResponseCode();
        assertThat(responseCode).isEqualTo(200);
        
        conn.disconnect();
    }
    
    /*@Test
    void testConnectEventOrdering() {
        UUID connId = UUID.randomUUID();
        
        // Отправляем события с нарушенным порядком
        NetworkEvent event2 = new NetworkEvent(connId, 2, EventType.DATA, "world".getBytes());
        NetworkEvent event1 = new NetworkEvent(connId, 1, EventType.DATA, "hello ".getBytes());
        NetworkEvent open = new NetworkEvent(connId, 0, EventType.OPEN, "httpbin.org:80".getBytes());
        
        clientEventConsumer.expectOrder(open, event1, event2);
        
        processor.accept(event2);
        processor.accept(event1);
        processor.accept(open);
        
        assertThat(clientEventConsumer.await(5, TimeUnit.SECONDS)).isTrue();
    }*/
}