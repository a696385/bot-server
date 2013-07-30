package name.away.bot.client;

import com.google.protobuf.ByteString;
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

import java.io.IOException;
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

    private static Client client = null;

    private static void tryConnect(){
        boolean clientConnected = false;
        int trying = 0;
        while (!clientConnected){
            trying++;
            try {
                client = new Client(clientHost, clientPort, host, port, "simple client", 10);
                clientConnected = true;
            } catch (IOException e) {
                log.error("Can not connect to server, try #{}", trying);
                clientConnected = false;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.error("Can not sleep thread", e1);
                }
            }
        }
    }

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
        while (true){
            if (client == null || !client.isConnected()) {
                tryConnect();
            }
            try {
                Thread.sleep(10000);
                client.getService().executeJob(client.getController(), ServerAPI.JobMessage.newBuilder().setId(-1).setName("test").addArgs(ByteString.copyFromUtf8("hello")).build(), new RpcCallback<ServerAPI.CompleteJobMessage>() {
                    @Override
                    public void run(ServerAPI.CompleteJobMessage completeJobMessage) {
                        log.info("Job Complete #{} - {}", completeJobMessage.getJobId(), completeJobMessage.getResult().toStringUtf8());
                    }
                });
            } catch (InterruptedException e) {
                log.error("Can not sleep thread", e);
            }
        }
    }
}
