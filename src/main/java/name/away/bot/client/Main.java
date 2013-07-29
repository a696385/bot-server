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
import org.apache.commons.cli.*;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.concurrent.Executors;

/**
 * Created by andy@away.name
 * Date: 29.07.13
 * Time: 20:12
 */
public class Main {

    private static Logger log;
    private static String host = "localhost";
    private static int port = 8080;
    private static String clientHost = "localhost";
    private static int clientPort = 8090;

    private static RpcClientChannel channel = null;
    private static String currentId = null;

    public static void main(String[] args) {
        /**
         * Configure log system
         */
        Configurator.initialize("config", null, "./config/log4j2.xml");
        java.util.logging.LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();

        log = LoggerFactory.getLogger(Main.class);
        log.info("Bot Worker v.0.0.1");
        /**
         * Load command line params
         *
         */
        Options options = new Options();
        options.addOption("h", "host", true, "Server host name");
        options.addOption("p", "port", true, "Server port");
        options.addOption("ch", "client-host", true, "Client host name");
        options.addOption("cp", "client-port", true, "Client port");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try{
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Can not parser params", e);
        }
        if (cmd != null){
            host = cmd.getOptionValue("h", host);
            port = Integer.parseInt(cmd.getOptionValue("p", String.valueOf(port)));
        }
        /**
         * Connect to RPC Server
         */
        PeerInfo client = new PeerInfo(clientHost, clientPort);
        final PeerInfo server = new PeerInfo(host, port);

        try {
            DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory(client);
            clientFactory.setConnectResponseTimeoutMillis(10000);
            RpcServerCallExecutor rpcExecutor = new ThreadPoolCallExecutor(3, 10);
            clientFactory.setRpcServerCallExecutor(rpcExecutor);

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
                }

                @Override
                public void connectionChanged(RpcClientChannel clientChannel) {
                    log.info("connectionChanged " + clientChannel);
                    channel = clientChannel;
                }
            };
            rpcEventNotifier.addEventListener(listener);
            clientFactory.registerConnectionEventListener(rpcEventNotifier);

            Bootstrap bootstrap = new Bootstrap();
            EventLoopGroup workers = new NioEventLoopGroup(16,new RenamingThreadFactoryProxy("workers", Executors.defaultThreadFactory()));

            bootstrap.group(workers);
            bootstrap.handler(clientFactory);
            bootstrap.channel(NioSocketChannel.class);
            bootstrap.option(ChannelOption.TCP_NODELAY, true);
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
            bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
            bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
            RpcClientConnectionWatchdog watchdog = new RpcClientConnectionWatchdog(clientFactory,bootstrap);
            rpcEventNotifier.addEventListener(watchdog);
            watchdog.start();

            CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
            shutdownHandler.addResource(bootstrap);
            shutdownHandler.addResource(rpcExecutor);

            clientFactory.peerWith(server, bootstrap);

            final ServerAPI.ServerAPIService.Interface service = ServerAPI.ServerAPIService.newStub(channel);
            final ClientRpcController controller = channel.newRpcController();
            controller.setTimeoutMs(0);

            channel.setOobMessageCallback(ServerAPI.GetJobsResponse.Jobs.getDefaultInstance(), new RpcCallback<Message>() {
                @Override
                public void run(Message message) {
                    ServerAPI.GetJobsResponse.Jobs job = null;
                    try {

                        job = ServerAPI.GetJobsResponse.Jobs.parseFrom(message.toByteArray());

                        if (job.hasGuid()){
                            currentId = job.getGuid();
                            ServerAPI.RegisterRequest.Builder registerRequest = ServerAPI.RegisterRequest.newBuilder();
                            registerRequest.setId(currentId).setMaxMpc(10);

                            service.register(controller, registerRequest.build(), new RpcCallback<ServerAPI.SuccessResponse>() {
                                @Override
                                public void run(ServerAPI.SuccessResponse successResponse) {
                                    if (successResponse.getSuccess()){
                                        log.info("Registered on server");
                                    } else {
                                        log.error("Can not register #{}", successResponse.getError());
                                    }
                                }
                            });

                            return;
                        }

                        while (currentId == null){
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                log.error("can not sleep thrad", e);
                            }
                        }

                        Worker worker = new Worker(job, channel, service, controller, currentId);
                        worker.run();

                    } catch (InvalidProtocolBufferException e) {
                        log.error("Can not parse message", e);
                    }
                }
            });

            while (channel != null) {

                Thread.sleep(10000);

            }

        } catch ( Exception e ) {
            log.warn("Failure.", e);
        }
    }
}
