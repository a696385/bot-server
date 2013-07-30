package name.away.bot.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import name.away.bot.api.ServerAPI;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

/**
 * Created by andy@away.name
 * Date: 29.07.13
 * Time: 22:29
 */
public class Worker {

    private ServerAPI.JobMessage job;
    private RpcClientChannel channel;
    private ServerAPI.ServerAPIService.Interface server;
    private ClientRpcController controller;
    private Logger log = LoggerFactory.getLogger(Worker.class);
    private String currentId;

    public Worker(ServerAPI.JobMessage job, RpcClientChannel channel, ServerAPI.ServerAPIService.Interface server, ClientRpcController controller, String currentId){

        this.job = job;
        this.channel = channel;
        this.server = server;
        this.controller = controller;
        this.currentId = currentId;
    }

    public void run(){
        log.info("Take job #{} ({})", job.getId(), job.getName());
        server.takeJobs(controller, ServerAPI.TakeJobMessage.newBuilder().setWorkerId(currentId).setJobId(job.getId()).build(), new RpcCallback<ServerAPI.SuccessResponseMessage>() {
            @Override
            public void run(ServerAPI.SuccessResponseMessage successResponse) {
                if (!successResponse.getSuccess()) {
                    log.error("Can not take job {}", job.getId());
                    return;
                }
                byte[] result = execute();
                log.info("Complete job #{}", job.getId());
                server.jobCompleted(controller, ServerAPI.CompleteJobMessage.newBuilder().setWorkerId(currentId).setJobId(job.getId()).setResult(ByteString.copyFrom(result)).build(), new RpcCallback<ServerAPI.SuccessResponseMessage>() {
                    @Override
                    public void run(ServerAPI.SuccessResponseMessage successResponse) {
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

    public String runCmd(String[] args) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(args));
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        InputStream is = proc.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line;
        int exit = -1;
        StringBuilder result = new StringBuilder("");

        while ((line = br.readLine()) != null) {
            // Outputs your process execution
            result.append(line + "\n");
            try {
                exit = proc.exitValue();
                if (exit == 0)  {
                    // Process finished
                }
            } catch (IllegalThreadStateException t) {
               proc.destroy();
            }
        }
        return result.toString();
    }

    private String downloadPage(String url, String cookies, String userAgent, String referer) throws IOException, URISyntaxException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        HttpGet method = new HttpGet(url);
        method.addHeader("Accept-Encoding", "gzip");
        if (!userAgent.isEmpty()) method.addHeader("User-Agent", userAgent);
        if (!referer.isEmpty()) method.addHeader("Referer", referer);
        if (!cookies.isEmpty()) method.addHeader("Cookie",  cookies);
        HttpResponse response = client.execute(method);
        try {
            HttpEntity entity = response.getEntity();
            InputStream entityContent = entity.getContent();
            Header contentEncoding = response.getFirstHeader("Content-Encoding");
            if (contentEncoding != null && contentEncoding.getValue().equalsIgnoreCase("gzip")) {
                entityContent = new GZIPInputStream(entityContent);
            }
            String str = null;
            String responseText = "";
            String charset = "UTF-8";
            Header contentType = response.getFirstHeader("Content-Type");
            try{
                charset = contentType.getValue().split(";")[1].split("=")[1];
            } catch (Exception ignored){ }
            BufferedReader d = new BufferedReader(new InputStreamReader(entityContent, charset));
            StringBuilder sb = new StringBuilder();
            while( (str = d.readLine()) !=null)
            {
                sb.append(str);
            }
            responseText = sb.toString();
            d.close();
            return responseText;
        } finally {
            method.releaseConnection();
        }
    }

    public byte[] execute(){
        try{
            if (job.getName().equalsIgnoreCase("exec")){
                ArrayList<String> args = new ArrayList<String>();
                for(int i = 0; i < job.getArgsCount(); i++){
                    args.add(job.getArgs(i).toStringUtf8());
                }
                try {
                    return runCmd(args.toArray(new String[args.size()])).getBytes();
                } catch (IOException e) {
                    return String.valueOf("ERROR :"+e.getMessage()).getBytes();
                }
            } else if (job.getName().equalsIgnoreCase("download")) {
                String url = job.getArgs(0).toStringUtf8();
                String cookies = job.getArgs(1).toStringUtf8();
                String ua = job.getArgs(2).toStringUtf8();
                String referer = job.getArgs(3).toStringUtf8();
                String result = downloadPage(url, cookies, ua, referer);
                return result.getBytes();
            }
            return String.valueOf("ERROR : COMMAND NOT FOUND").getBytes();
        } catch (Exception ex){
            return String.valueOf("ERROR :" + ex.getMessage()).getBytes();
        }
    }
}
