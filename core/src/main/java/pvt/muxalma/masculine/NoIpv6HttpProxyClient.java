package pvt.muxalma.masculine;

import java.net.URISyntaxException;
import java.util.function.Consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import pvt.muxalma.model.NetworkEvent;

public class NoIpv6HttpProxyClient extends HttpProxyClient {

    private final NioEventLoopGroup dnsEventLoopGroup;

    public NoIpv6HttpProxyClient(Consumer<NetworkEvent> upstreamConsumer) {
        super(upstreamConsumer);
        this.dnsEventLoopGroup = new NioEventLoopGroup(1);
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

        DnsNameResolverBuilder resolverBuilder = new DnsNameResolverBuilder()
                .eventLoop(dnsEventLoopGroup.next())
                .datagramChannelType(NioDatagramChannel.class)
                .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY);

        DnsAddressResolverGroup resolverGroup = new DnsAddressResolverGroup(resolverBuilder);

        return bootstrap.resolver(resolverGroup);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        dnsEventLoopGroup.shutdownGracefully();
    }
}
