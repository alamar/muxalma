package pvt.muxalma.masculine;

import java.net.URISyntaxException;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DefaultDnsCache;
import io.netty.resolver.dns.DefaultDnsCnameCache;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsCache;
import io.netty.resolver.dns.DnsCnameCache;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import pvt.muxalma.model.NetworkEvent;

public class NoIpv6HttpProxyClient extends HttpProxyClient {

    private final NioEventLoopGroup dnsEventLoopGroup;
    private final DnsAddressResolverGroup resolverGroup;

    public NoIpv6HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        super(upstreamConsumer);

        this.dnsEventLoopGroup = new NioEventLoopGroup(1);

        DnsCache dnsCache = new DefaultDnsCache(60, 300, 30);
        DnsCnameCache dnsCnameCache = new DefaultDnsCnameCache(60, 300);

        DnsNameResolverBuilder resolverBuilder = new DnsNameResolverBuilder()
                .eventLoop(dnsEventLoopGroup.next())
                .datagramChannelType(NioDatagramChannel.class)
                .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY)
                .resolveCache(dnsCache)
                .cnameCache(dnsCnameCache)
                .maxQueriesPerResolve(2)
                .queryTimeoutMillis(3000);

        resolverGroup = new DnsAddressResolverGroup(resolverBuilder);
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
        Bootstrap bootstrap = super.configureBootstrap();
        return bootstrap.resolver(resolverGroup);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        resolverGroup.close();
        dnsEventLoopGroup.shutdownGracefully();
    }
}
