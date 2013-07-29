package name.away.bot.server;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.sun.xml.internal.ws.util.ByteArrayBuffer;
import name.away.bot.api.ServerAPI;

import java.util.*;

/**
 * Created by andy@away.name
 * Date: 29.07.13
 * Time: 21:36
 */
public class Store {

    private class TackedJobInfo{
        private BotInfo worker;
        private Boolean completed;

        public TackedJobInfo(BotInfo worker, Boolean completed){
            this.worker = worker;
            this.completed = completed;
        }
    }

    public class BotInfo {
        private RpcClientChannel chanel;
        private String id;
        private int maxCmp;
        private int nowWorked = 0;
        private boolean canExecute = false;

        public BotInfo(RpcClientChannel chanel, String id, int maxCmp){

            this.chanel = chanel;
            this.id = id;
            this.maxCmp = maxCmp;
        }

        public RpcClientChannel getChanel() {
            return chanel;
        }

        public String getId() {
            return id;
        }

        public void incNowWorked(int delta){
            nowWorked += delta;
        }

        public int getMaxCmp() {
            return maxCmp;
        }

        public void setMaxCmp(int maxCmp) {
            this.maxCmp = maxCmp;
        }

        public int getNowWorked() {
            return nowWorked;
        }

        public boolean isCanExecute() {
            return canExecute;
        }

        public void setCanExecute(boolean canExecute) {
            this.canExecute = canExecute;
        }
    }

    LinkedHashMap<Long, ServerAPI.GetJobsResponse.Jobs> jobs = new LinkedHashMap<Long, ServerAPI.GetJobsResponse.Jobs>();
    LinkedHashMap<Long, TackedJobInfo> tackedJobs = new LinkedHashMap<Long, TackedJobInfo>();
    LinkedHashMap<Long, ByteArrayBuffer> completedJobs = new LinkedHashMap<Long, ByteArrayBuffer>();
    LinkedList<BotInfo> bots = new LinkedList<BotInfo>();

    public Store(){

    }

    public synchronized void registerBot(RpcClientChannel chanel, String id, int maxCmp){
        bots.add(new BotInfo(chanel, id, maxCmp));
    }

    public synchronized BotInfo findBot(String id){
        for(BotInfo info : bots){
            if (info.getId().equals(id)) return info;
        }
        return null;
    }

    public synchronized boolean removeBot(String id){
        for(BotInfo info : bots){
            if (info.getId().equals(id)){
                bots.remove(info);
                return true;
            }
        }
        return false;
    }

    public synchronized long addJob(String name, String[] args){
        long Id = jobs.size() + 1;
        ServerAPI.GetJobsResponse.Jobs job = ServerAPI.GetJobsResponse.Jobs
                .newBuilder()
                .setId(Id)
                .setName(name)
                .addAllArgs(Arrays.asList(args)).build();
        jobs.put(Id, job);
        return Id;
    }

    public synchronized boolean tackJob(Long jobId, String workerId){
        if (jobs.get(jobId) == null) return false;
        BotInfo worker = findBot(workerId);
        if (worker == null) return false;
        tackedJobs.put(jobId, new TackedJobInfo(worker, false));
        worker.incNowWorked(1);
        return true;
    }

    public synchronized List<ServerAPI.GetJobsResponse.Jobs> getFreeJobs(){
        LinkedList<ServerAPI.GetJobsResponse.Jobs> result = new LinkedList<ServerAPI.GetJobsResponse.Jobs>();
        for(long key : jobs.keySet()){
            if (tackedJobs.get(key) != null) continue;
            result.add(jobs.get(key));
        }
        return result;
    }

    public synchronized ServerAPI.GetJobsResponse.Jobs getJob(long jobId){
        if (tackedJobs.get(jobId) != null) return null;
        return jobs.get(jobId);
    }

    public synchronized boolean completedJob(long jobId, ByteArrayBuffer data){
        TackedJobInfo info = tackedJobs.get(jobId);
        if (info == null || info.completed) return false;
        info.completed = true;
        completedJobs.put(jobId, data);
        info.worker.incNowWorked(-1);
        return true;
    }

    public synchronized ByteArrayBuffer getJobResult(long jobId){
        return completedJobs.get(jobId);
    }
}
