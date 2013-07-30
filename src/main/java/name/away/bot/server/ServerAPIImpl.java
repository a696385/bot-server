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

    private ServerAPI.SuccessResponseMessage buildSuccess(boolean value, int errorCode){
        return  ServerAPI.SuccessResponseMessage.newBuilder().setSuccess(value).setError(errorCode).build();
    }

    @Override
    public void registerWorker(RpcController controller, ServerAPI.RegisterWorkerMessage request, RpcCallback<ServerAPI.SuccessResponseMessage> done) {
        Store.BotInfo info = store.findBot(request.getId());
        if (info == null){
            done.run(buildSuccess(false, 0));
            return;
        }
        info.setMaxRunAtTime(request.getMaxOneTimeRequests());
        info.setName(request.getName());
        info.setCanExecute(true);
        done.run(buildSuccess(true, -1));
        log.info("Register: {}", info.getId());
        ServerAPI.JobsMessage.Builder jobs = ServerAPI.JobsMessage.newBuilder();
        for(ServerAPI.JobMessage job : store.getFreeJobs("")){
            sendNotificationOfNewJob(job);
        }
    }

    @Override
    public void removeWorker(RpcController controller, ServerAPI.RemoveWorkerMessage request, RpcCallback<ServerAPI.SuccessResponseMessage> done) {
        if (store.removeBot(request.getId())){
            log.info("Unregistered: {}", request.getId());
            done.run(buildSuccess(true, -1));
        } else {
            done.run(buildSuccess(false, 1));
        }
    }

    @Override
    public void getJobs(RpcController controller, ServerAPI.WorkerId request, RpcCallback<ServerAPI.JobsMessage> done) {
        ServerAPI.JobsMessage.Builder jobs = ServerAPI.JobsMessage.newBuilder();
        for(ServerAPI.JobMessage job : store.getFreeJobs(request.getWorkerId())){
            jobs.addJob(job);
        }
        done.run(jobs.build());
    }

    @Override
    public void takeJobs(RpcController controller, ServerAPI.TakeJobMessage request, RpcCallback<ServerAPI.SuccessResponseMessage> done) {
        if (store.tackJob(request.getJobId(), request.getWorkerId())){
            log.info("{} Take job - {}", request.getWorkerId(), request.getJobId());
            done.run(buildSuccess(true, -1));
        } else {
            done.run(buildSuccess(false, 2));
        }
    }

    @Override
    public void jobCompleted(RpcController controller, ServerAPI.CompleteJobMessage request, RpcCallback<ServerAPI.SuccessResponseMessage> done) {
        if (store.completedJob(request.getJobId(), new ByteArrayBuffer(request.getResult().toByteArray()))){
            done.run(buildSuccess(true, -1));
        } else {
            done.run(buildSuccess(false, 3));
        }
        for(ServerAPI.JobMessage job : store.getFreeJobs("")){
            sendNotificationOfNewJob(job);
        }
    }

    private void sendNotificationOfNewJob(ServerAPI.JobMessage job){
        for(RpcClientChannel channel: clientRegistry.getAllClients()){
            Store.BotInfo bi = store.findBot(channel.getPeerInfo().getPid());
            if (bi == null) continue;
            if (!bi.isCanExecute()) continue;
            if (bi.getMaxRunAtTime() <= bi.getNowWorked()) continue;
            if (job.hasWorkerId() && !bi.getId().equals(job.getWorkerId())) continue;;

            ServerAPI.ClientNotification.Builder notification = ServerAPI.ClientNotification.newBuilder();
            notification.setType("new-job");
            notification.setData(job.toByteString());
            channel.sendOobMessage(notification.build());
        }
    }

    @Override
    public void executeJob(RpcController controller, ServerAPI.JobMessage request, RpcCallback<ServerAPI.CompleteJobMessage> done) {
        long jobId = store.addJob(request.getName(),request.getArgsList().toArray(new ByteString[request.getArgsCount()]));
        Store.JobContainer storeJob = store.getJob(jobId);
        sendNotificationOfNewJob(storeJob.getJob());

        ByteArrayBuffer buff = null;
        try {
            storeJob.getCompleteWaiter().wait();
            buff = store.getJobResult(jobId);
        } catch (InterruptedException e) {
            log.error("Can not wait for complete job", e);
        }
        if (buff != null){
            done.run(ServerAPI.CompleteJobMessage.newBuilder().setJobId(jobId).setResult(ByteString.copyFrom(buff.getRawData())).build());
        } else {
            done.run(ServerAPI.CompleteJobMessage.newBuilder().setJobId(jobId).setResult(ByteString.copyFromUtf8("")).build());
        }
    }
}
