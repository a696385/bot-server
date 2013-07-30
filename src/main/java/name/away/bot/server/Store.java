package name.away.bot.server;

import com.google.protobuf.ByteString;
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
        private String name;
        private int maxRunAtTime;
        private int nowWorked = 0;
        private boolean canExecute = false;

        public BotInfo(RpcClientChannel chanel, String id, String name, int maxRunAtTime){

            this.chanel = chanel;
            this.id = id;
            this.name = name;
            this.maxRunAtTime = maxRunAtTime;
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

        public int getNowWorked() {
            return nowWorked;
        }

        public boolean isCanExecute() {
            return canExecute;
        }

        public void setName(String name){
            this.name = name;
        }

        public String getName(){
            return this.name;
        }

        public void setCanExecute(boolean canExecute) {
            this.canExecute = canExecute;
        }

        public int getMaxRunAtTime() {
            return maxRunAtTime;
        }

        public void setMaxRunAtTime(int maxRunAtTime) {
            this.maxRunAtTime = maxRunAtTime;
        }

        @Override
        public String toString(){
            return name;
        }
    }

    public class JobContainer {

        private ServerAPI.JobMessage job;
        private Object completeWaiter = new Object();

        public JobContainer(ServerAPI.JobMessage job){

            this.job = job;
        }

        public ServerAPI.JobMessage getJob() {
            return job;
        }

        public Object getCompleteWaiter() {
            return completeWaiter;
        }
    }

    LinkedHashMap<Long, JobContainer> jobs = new LinkedHashMap<Long, JobContainer>();
    LinkedHashMap<Long, TackedJobInfo> tackedJobs = new LinkedHashMap<Long, TackedJobInfo>();
    LinkedHashMap<Long, ByteArrayBuffer> completedJobs = new LinkedHashMap<Long, ByteArrayBuffer>();
    LinkedList<BotInfo> bots = new LinkedList<BotInfo>();

    public Store(){

    }

    public synchronized void registerBot(RpcClientChannel chanel, String id, int maxCmp, String name){
        bots.add(new BotInfo(chanel, id, name, maxCmp));
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

    public synchronized long addJob(String name, ByteString[] args){
        long Id = jobs.size() + 1;
        ServerAPI.JobMessage job = ServerAPI.JobMessage
                .newBuilder()
                .setId(Id)
                .setName(name)
                .addAllArgs(Arrays.asList(args)).build();
        jobs.put(Id, new JobContainer(job));
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

    public synchronized List<ServerAPI.JobMessage> getFreeJobs(String workerId){
        LinkedList<ServerAPI.JobMessage> result = new LinkedList<ServerAPI.JobMessage>();
        for(long key : jobs.keySet()){
            ServerAPI.JobMessage job = jobs.get(key).getJob();
            if (tackedJobs.get(key) != null) continue;
            if (!workerId.isEmpty() && job.hasWorkerId() && !job.getWorkerId().equals(workerId)) continue;
            result.add(job);
        }
        return result;
    }

    public synchronized JobContainer getJob(long jobId){
        if (tackedJobs.get(jobId) != null) return null;
        return jobs.get(jobId);
    }

    public synchronized boolean completedJob(long jobId, ByteArrayBuffer data){
        TackedJobInfo info = tackedJobs.get(jobId);
        if (info == null || info.completed) return false;
        info.completed = true;
        completedJobs.put(jobId, data);
        info.worker.incNowWorked(-1);

        Object completeWaiter = getJob(jobId).getCompleteWaiter();
        completeWaiter.notifyAll();

        return true;
    }

    public synchronized ByteArrayBuffer getJobResult(long jobId){
        return completedJobs.get(jobId);
    }
}
