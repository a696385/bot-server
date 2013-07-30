package name.away.bot.server;

import com.google.protobuf.ByteString;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import name.away.bot.api.ServerAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcConnectionEventer implements RpcConnectionEventListener {

    private Store store;
    private Logger log = LoggerFactory.getLogger(RpcConnectionEventer.class);

    public RpcConnectionEventer(Store store){
        this.store = store;
    }

    @Override
    public void connectionLost(RpcClientChannel rpcClientChannel) {
        store.removeBot(rpcClientChannel.getPeerInfo().getPid());
    }

    @Override
    public void connectionOpened(final RpcClientChannel rpcClientChannel) {
        final String clientId = rpcClientChannel.getPeerInfo().getPid();
        store.registerBot(rpcClientChannel, clientId, 10, "");
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    log.error("Can not sleep thread", e);
                }
                ServerAPI.WorkerId workerId = ServerAPI.WorkerId.newBuilder().setWorkerId(clientId).build();
                ServerAPI.ClientNotification.Builder notification = ServerAPI.ClientNotification.newBuilder();
                notification.setType("worker-id");
                notification.setData(workerId.toByteString());
                rpcClientChannel.sendOobMessage(notification.build());
            }
        });
        th.run();
    }

    @Override
    public void connectionReestablished(RpcClientChannel rpcClientChannel) {

    }

    @Override
    public void connectionChanged(RpcClientChannel rpcClientChannel) {
        this.connectionLost(rpcClientChannel);
        this.connectionOpened(rpcClientChannel);
    }
}
