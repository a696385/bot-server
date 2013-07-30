package name.away.bot.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.*;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import com.googlecode.protobuf.pro.duplex.client.RpcClientConnectionWatchdog;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import name.away.bot.api.ServerAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;

public class Client {

    private String clientHost;
    private int clientPort;
    private String host;
    private int port;
    private Logger log = LoggerFactory.getLogger(Client.class);
    private RpcClientChannel channel;
    private String currentId;
    private String name;
    private int maxJobsPerTime;
    private Bootstrap bootstrap;
    private ServerAPI.ServerAPIService.Interface service;
    private ClientRpcController controller;
    private RpcClientConnectionWatchdog watchdog;

    public Client(String clientHost, int clientPort, String host, int port, final String name, final int maxJobsPerTime) throws IOException {
        this.clientHost = clientHost;
        this.clientPort = clientPort;
        this.host = host;
        this.port = port;
        this.name = name;
        this.maxJobsPerTime = maxJobsPerTime;

        PeerInfo client = new PeerInfo(this.clientHost, this.clientPort);
        final PeerInfo server = new PeerInfo(this.host, this.port);


        DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory(client);
        clientFactory.setConnectResponseTimeoutMillis(10000);
        RpcServerCallExecutor rpcExecutor = new ThreadPoolCallExecutor(3, 10);
        clientFactory.setRpcServerCallExecutor(rpcExecutor);

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();

        // RPC payloads are uncompressed when logged - so reduce logging
        CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
        logger.setLogRequestProto(false);
        logger.setLogResponseProto(false);
        clientFactory.setRpcLogger(logger);
        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();

        final RpcConnectionEventListener listener = new RpcConnectionEventListener() {

            @Override
            public void connectionReestablished(RpcClientChannel clientChannel) {
                log.info("connectionReestablished " + clientChannel);
                channel = clientChannel;
            }

            @Override
            public void connectionOpened(RpcClientChannel clientChannel) {
                log.info("connectionOpened " + clientChannel);
                channel = clientChannel;
            }

            @Override
            public void connectionLost(RpcClientChannel clientChannel) {
                log.info("connectionLost " + clientChannel);
                channel = null;
                bootstrap.shutdown();
            }

            @Override
            public void connectionChanged(RpcClientChannel clientChannel) {
                log.info("connectionChanged " + clientChannel);
                channel = clientChannel;
            }
        };
        rpcEventNotifier.addEventListener(listener);
        clientFactory.registerConnectionEventListener(rpcEventNotifier);

        bootstrap = new Bootstrap();
        EventLoopGroup workers = new NioEventLoopGroup(16,new RenamingThreadFactoryProxy("workers", Executors.defaultThreadFactory()));

        bootstrap.group(workers);
        bootstrap.handler(clientFactory);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

        watchdog = new RpcClientConnectionWatchdog(clientFactory, bootstrap);
        rpcEventNotifier.addEventListener(watchdog);
        watchdog.start();


        shutdownHandler.addResource(bootstrap);
        shutdownHandler.addResource(rpcExecutor);

        clientFactory.peerWith(server, bootstrap);

        service = ServerAPI.ServerAPIService.newStub(channel);
        controller = channel.newRpcController();
        controller.setTimeoutMs(0);
        channel.setOobMessageCallback(ServerAPI.ClientNotification.getDefaultInstance(), new RpcCallback<Message>() {
            @Override
            public void run(Message message) {
                ServerAPI.ClientNotification notification = null;
                try {

                    notification = ServerAPI.ClientNotification.parseFrom(message.toByteArray());
                    if (notification.getType().equals("worker-id")){
                        ServerAPI.WorkerId workerId =  ServerAPI.WorkerId.parseFrom(notification.getData());
                        currentId = workerId.getWorkerId();


                        ServerAPI.RegisterWorkerMessage.Builder registerRequest = ServerAPI.RegisterWorkerMessage.newBuilder();
                        registerRequest.setId(currentId).setMaxOneTimeRequests(maxJobsPerTime).setName(name);
                        service.registerWorker(controller, registerRequest.build(), new RpcCallback<ServerAPI.SuccessResponseMessage>() {
                            @Override
                            public void run(ServerAPI.SuccessResponseMessage successResponse) {
                                if (successResponse.getSuccess()){
                                    log.info("Registered on server");
                                } else {
                                    log.error("Can not register #{}", successResponse.getError());
                                }
                            }
                        });
                    } else if (notification.getType().equals("new-job")){
                        ServerAPI.JobMessage job = ServerAPI.JobMessage.parseFrom(notification.getData());
                        Worker worker = new Worker(job, channel, service, controller, currentId);
                        worker.run();
                    }

                } catch (InvalidProtocolBufferException e) {
                    log.error("Can not parse message", e);
                }
            }
        });
    }

    public boolean isConnected(){
        return channel != null;
    }

    public ServerAPI.ServerAPIService.Interface getService(){
        return service;
    }

    public ClientRpcController getController(){
        return controller;
    }
}
