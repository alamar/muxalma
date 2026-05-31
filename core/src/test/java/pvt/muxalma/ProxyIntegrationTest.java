package pvt.muxalma;

import org.junit.jupiter.api.*;
import pvt.muxalma.feminine.ConnectionManager;
import pvt.muxalma.feminine.HttpProxyServer;
import pvt.muxalma.masculine.HttpProxyClient;
import pvt.muxalma.masculine.OrderingProcessor;
import pvt.muxalma.masculine.ParallelProcessor;
import pvt.muxalma.model.NetworkEvent;
import pvt.muxalma.model.ValidationProcessor;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ProxyIntegrationTest {
    
    private HttpProxyServer server;
    private TestEventConsumer clientEventConsumer;
    private HttpProxyClient client;
    private ParallelProcessor parallel;
    
    @BeforeAll
    void setup() throws InterruptedException {
        clientEventConsumer = new TestEventConsumer();
        client = new HttpProxyClient(clientEventConsumer);
        // Создаём цепочку для обработки событий
        OrderingProcessor ordered = new OrderingProcessor(client);
        parallel = new ParallelProcessor(ordered);
        Consumer<NetworkEvent> valid = new ValidationProcessor(
                parallel, clientEventConsumer);
        server = new HttpProxyServer(18080, valid, new ConnectionManager());
        server.start();
        
        // Даем время на запуск
        Thread.sleep(1000);
    }
    
    @AfterAll
    void cleanup() {
        server.stop();
        client.shutdown();
        parallel.shutdown();
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
    
    private static class TestEventConsumer implements Consumer<NetworkEvent> {
        private final AtomicReference<NetworkEvent[]> expected = new AtomicReference<>();
        private final CountDownLatch latch = new CountDownLatch(3);
        private int index = 0;
        
        void expectOrder(NetworkEvent... events) {
            expected.set(events);
        }
        
        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
        
        @Override
        public void accept(NetworkEvent event) {
            NetworkEvent[] expectedEvents = expected.get();
            if (expectedEvents != null && index < expectedEvents.length) {
                assertThat(event.getSerial()).isEqualTo(expectedEvents[index].getSerial());
                assertThat(event.getType()).isEqualTo(expectedEvents[index].getType());
                index++;
                latch.countDown();
            }
        }
    }
}