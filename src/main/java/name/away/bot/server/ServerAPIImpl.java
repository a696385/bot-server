package name.away.bot.server;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import name.away.bot.api.ServerAPI;

/**
 * Created by Andy <andy@away.name>
 * Date: 7/29/13
 */
public class ServerAPIImpl extends ServerAPI.ServerAPIService {
    @Override
    public void register(RpcController controller, ServerAPI.RegisterRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void unRegister(RpcController controller, ServerAPI.RequestId request, RpcCallback<ServerAPI.SuccessResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void getJobs(RpcController controller, ServerAPI.RequestId request, RpcCallback<ServerAPI.GetJobsResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void takeJobs(RpcController controller, ServerAPI.TackJobRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void jobCompleted(RpcController controller, ServerAPI.JobCompletedRequest request, RpcCallback<ServerAPI.SuccessResponse> done) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
