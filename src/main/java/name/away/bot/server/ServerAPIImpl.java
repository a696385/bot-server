package name.away.bot.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.sun.xml.internal.ws.util.ByteArrayBuffer;
import name.away.bot.api.ServerAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Andy <andy@away.name>
 * Date: 7/29/13
 */
public class ServerAPIImpl extends ServerAPI.ServerAPIService {
    private Logger log = LoggerFactory.getLogger(ServerAPIImpl.class);
    private Store store;
    private RpcClientRegistry clientRegistry;

    public ServerAPIImpl(Store store, RpcClientRegistry clientRegistry){
        this.store = store;
        this.clientRegistry = clientRegistry;
    }

    @Override
    public void register(RpcController controller, ServerAPI.RegisterRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        Store.BotInfo info = store.findBot(request.getId());
        if (info == null){
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(false).build());
            return;
        }
        info.setMaxCmp(request.getMaxMpc());
        info.setCanExecute(true);
        log.info("Register: {}", info.getId());
        done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(true).build());
    }

    @Override
    public void unRegister(RpcController controller, ServerAPI.RequestId request, RpcCallback<ServerAPI.SuccessResponse> done) {
        if (store.removeBot(request.getId())){
            log.info("Unregistered: {}", request.getId());
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(true).build());
        } else {
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(false).build());
        }
    }

    @Override
    public void getJobs(RpcController controller, ServerAPI.RequestId request, RpcCallback<ServerAPI.GetJobsResponse> done) {
        ServerAPI.GetJobsResponse.Builder response = ServerAPI.GetJobsResponse.newBuilder();
        for(ServerAPI.GetJobsResponse.Jobs job : store.getFreeJobs()){
            response.addJobs(job);
        }
        done.run(response.build());
    }

    @Override
    public void takeJobs(RpcController controller, ServerAPI.TackJobRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        if (store.tackJob(request.getJobId(), request.getId())){
            log.info("{} Take job - {}", request.getId(), request.getJobId());
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(true).build());
        } else {
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(false).build());
        }
    }

    @Override
    public void jobCompleted(RpcController controller, ServerAPI.JobCompletedRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        if (store.completedJob(request.getJobId(), new ByteArrayBuffer(request.getResult().toByteArray()))){
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(true).build());
        } else {
            done.run(ServerAPI.SuccessResponse.newBuilder().setSuccess(false).build());
        }
    }

    private void sendNotification(ServerAPI.GetJobsResponse.Jobs job){
        for(RpcClientChannel channel: clientRegistry.getAllClients()){
            Store.BotInfo bi = store.findBot(channel.getPeerInfo().getPid());
            if (bi == null) continue;
            if (!bi.isCanExecute()) continue;
            if (bi.getMaxCmp() <= bi.getNowWorked()) continue;
            channel.sendOobMessage(job);
        }
    }

    @Override
    public void executeJob(RpcController controller, ServerAPI.JobRequest request, RpcCallback<ServerAPI.JobResponse> done) {
        long jobId = store.addJob(request.getName(),request.getArgsList().toArray(new String[request.getArgsCount()]));
        ServerAPI.GetJobsResponse.Jobs storeJob = store.getJob(jobId);
        sendNotification(storeJob);

        ByteArrayBuffer buff = null;
        while (true){
            buff = store.getJobResult(jobId);
            if (buff != null) break;
            try {
                Thread.sleep(100);
                sendNotification(storeJob);
            } catch (InterruptedException e) {
                log.error("Can not sleep thread", e);
            }
        }
        done.run(ServerAPI.JobResponse.newBuilder().setResult(ByteString.copyFrom(buff.getRawData())).build());

    }
}
