package name.away.bot.server;

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
import org.apache.commons.cli.*;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;


import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created by Andy <andy@away.name>
 * Date: 7/29/13
 */
public class Main {

    private static Logger log;
    private static String host = "localhost";
    private static int port = 8080;

    public static void main(String[] args) {
        /**
         * Configure log system
         */
        Configurator.initialize("config", null, "./config/log4j2.xml");
        java.util.logging.LogManager.getLogManager().reset();
        SLF4JBridgeHandler.install();

        log = LoggerFactory.getLogger(Main.class);
        log.info("Bot server v.0.0.1");
        /**
         * Load command line params
         *
         */
        Options options = new Options();
        options.addOption("h", "host", true, "Server host name");
        options.addOption("p", "port", true, "Server port");

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
         * Config store
         */
        final Store store = new Store();
        /**
         * Start RPC Protobuf Server
         */
        Server server = new Server(host, port, store);
        // Bind and start to accept incoming connections.
        server.run();
        log.info("Serving {}", server);

        /**
         * Main loop
         */
        while ( true ) {

            log.info("Number of clients {}", server.getClientCount());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                log.error("Can not sleep main thread", e);
            }
        }
    }
}
