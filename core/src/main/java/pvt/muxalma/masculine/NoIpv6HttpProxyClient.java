package pvt.muxalma.masculine;

import java.net.URISyntaxException;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import pvt.muxalma.model.NetworkEvent;

public class NoIpv6HttpProxyClient extends HttpProxyClient {

    public NoIpv6HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        super(upstreamConsumer);
    }

    @Override
    protected HostPort resolveHostPort(ConnectionEvent event) throws URISyntaxException, IllegalArgumentException {
        HostPort hostPort = super.resolveHostPort(event);
        if (hostPort.isIPv6())
            throw new IllegalCallerException("Disallowed address type");
        return hostPort;
    }

    @Override
    protected Bootstrap configureBootstrap() {
        return new Bootstrap().resolver(new DnsAddressResolverGroup(new DnsNameResolverBuilder()
                .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY)));
    }
}
