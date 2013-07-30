package name.away.bot.server;

import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import name.away.bot.api.ServerAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;

public class Server {

    private String host;
    private int port;
    private Logger log = LoggerFactory.getLogger(Server.class);
    private Store store;
    private DuplexTcpServerPipelineFactory serverFactory;
    private ServerBootstrap bootstrap;

    public Server(String host, int port, Store store){
        this.host = host;
        this.port = port;
        this.store = store;
        PeerInfo serverInfo = new PeerInfo(this.host, this.port);
        CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
        logger.setLogRequestProto(false);
        logger.setLogResponseProto(false);

        serverFactory = new DuplexTcpServerPipelineFactory(serverInfo);
        RpcServerCallExecutor rpcExecutor = new ThreadPoolCallExecutor(10, 10);
        serverFactory.setRpcServerCallExecutor(rpcExecutor);
        serverFactory.setLogger(logger);

        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        rpcEventNotifier.setEventListener(new RpcConnectionEventer(store));
        serverFactory.registerConnectionEventListener(rpcEventNotifier);

        serverFactory.getRpcServiceRegistry().registerService(new ServerAPIImpl(this.store, serverFactory.getRpcClientRegistry()));

        bootstrap = new ServerBootstrap();
        EventLoopGroup boss = new NioEventLoopGroup(4,new RenamingThreadFactoryProxy("boss", Executors.defaultThreadFactory()));
        EventLoopGroup workers = new NioEventLoopGroup(4,new RenamingThreadFactoryProxy("worker", Executors.defaultThreadFactory()));
        bootstrap.group(boss,workers);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childHandler(serverFactory);
        bootstrap.localAddress(serverInfo.getPort());

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(bootstrap);
        shutdownHandler.addResource(rpcExecutor);

    }

    public void run(){
        bootstrap.bind();
    }

    @Override
    public String toString(){
        return bootstrap.toString();
    }

    public int getClientCount(){
        return serverFactory.getRpcClientRegistry().getAllClients().size();
    }
}
