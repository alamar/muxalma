package pvt.muxalma.masculine;

import java.net.URI;
import java.net.URISyntaxException;

import io.netty.util.NetUtil;

public class HostPort {
        private final String host;
        private final int port;
        
        public HostPort(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public HostPort(String connectionString) throws URISyntaxException, IllegalArgumentException {
            if (connectionString == null || connectionString.isEmpty()) {
                throw new IllegalArgumentException("Empty connection string");
            }

            String uriString = connectionString;
            // XXX Вроде бы HttpsProxyServer не должен такое присылать
            boolean isHttps = uriString.startsWith("https://");
            if (!uriString.startsWith("http://") && !isHttps) {
                uriString = "http://" + uriString;
            }

            URI uri = new URI(uriString);
            String host = uri.getHost();
            int port = uri.getPort();

            if (host == null) {
                // Может быть просто "host" без порта?
                host = connectionString;
                port = isHttps ? 443 : 80;
            }

            if (port == -1) {
                port = isHttps ? 443 : 80;
            }

            this.host = host;
            this.port = port;
        }
        
        public String getHost() { return host; }
        public int getPort() { return port; }
        
        public boolean isIPv6() {
            return NetUtil.isValidIpV6Address(host);
        }
        
        public boolean isIPv4() {
            return NetUtil.isValidIpV4Address(host);
        }
        
        public boolean isDomainName() {
            return !isIPv4() && !isIPv6();
        }
        
        @Override
        public String toString() {
            if (isIPv6()) {
                return "[" + host + "]:" + port;
            }
            return host + ":" + port;
        }
    }