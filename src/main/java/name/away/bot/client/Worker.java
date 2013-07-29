package name.away.bot.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import name.away.bot.api.ServerAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by andy@away.name
 * Date: 29.07.13
 * Time: 22:29
 */
public class Worker {

    private ServerAPI.GetJobsResponse.Jobs job;
    private RpcClientChannel channel;
    private ServerAPI.ServerAPIService.Interface server;
    private ClientRpcController controller;
    private Logger log = LoggerFactory.getLogger(Worker.class);
    private String currentId;

    public Worker(ServerAPI.GetJobsResponse.Jobs job, RpcClientChannel channel, ServerAPI.ServerAPIService.Interface server, ClientRpcController controller, String currentId){

        this.job = job;
        this.channel = channel;
        this.server = server;
        this.controller = controller;
        this.currentId = currentId;
    }

    public void run(){
        log.info("Take job #{} ({})", job.getId(), job.getName());
        server.takeJobs(controller, ServerAPI.TackJobRequest.newBuilder().setId(currentId).setJobId(job.getId()).build(), new RpcCallback<ServerAPI.SuccessResponse>() {
            @Override
            public void run(ServerAPI.SuccessResponse successResponse) {
                if (!successResponse.getSuccess()) {
                    log.error("Can not take job {}", job.getId());
                    return;
                }
                byte[] result = execute();
                log.info("Complete job #{}", job.getId());
                server.jobCompleted(controller, ServerAPI.JobCompletedRequest.newBuilder().setId(currentId).setJobId(job.getId()).setResult(ByteString.copyFrom(result)).build(), new RpcCallback<ServerAPI.SuccessResponse>() {
                    @Override
                    public void run(ServerAPI.SuccessResponse successResponse) {
                        if (!successResponse.getSuccess()) {
                            log.error("Can complete job #{}", job.getId());
                        } else {
                            log.info("Job completed #{}", job.getId());
                        }
                    }
                });
            }
        });
    }

    public byte[] execute(){
        return "Hello".getBytes();
    }
}
