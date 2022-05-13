package edu.upenn.cis.cis455.crawler.master;

import static spark.Spark.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MasterServer {
    final static Logger logger = LogManager.getLogger(MasterServer.class);
    public static ConcurrentMap<String, Long> workerStatus = new ConcurrentHashMap<>();
    public static ConcurrentMap<String, String> progress = new ConcurrentHashMap<>();
    public static List<String> workers = new ArrayList<>();
    public static LoadBalancer loadBalancer = new LoadBalancer();

    public static void registerHandlers() {
        get("/workerstatus", (request, response) -> {
            String ip = request.ip();
            String port = request.queryParams("port");
            String addr = ip + ":" + port;
            String indexed = request.queryParams("indexed");
            logger.info("received msg from " + addr + "; Indexed: " + indexed);
            MasterHelper.setActiveWorkers(addr, indexed);
            return "";
        });

        get("/status", (request, response) -> {
            String body = "<HTML><BODY><h3>Crawler is working</h3>";
            body += "<div>";
            for (String worker : progress.keySet()) {
                if (!workerStatus.containsKey(worker)) {
                    continue;
                }
                Long time = workerStatus.get(worker);
                body += "<div>For worker " + worker + "<br>";
                body += "<ul><li>Last report: " + MasterHelper.format(time) + "</li>";
                body += "<li>Indexed pages: " + progress.get(worker) + "</li>";
                body += "</ul></div>";
            }
            body += "</div></BODY></HTML>";
            response.type("text/html");
            response.body(body);
            return response.body();
        });

        get("/sendWork", (request, response) -> {
            String ip = request.ip();
            String port = request.queryParams("port");
            String addr = ip + ":" + port;
            logger.info(addr + " asks for more work");
            // loadBalancer.sendWikiWork();
            loadBalancer.sendOtherWork();
            return "";
        });

        get("/stop", (request, response) -> {
            logger.info("try stopping");
            notifyAll("stop");
            return "";
        });

        get("/shutdown", (request, response) -> {
            logger.info("try shuttding down");
            notifyAll("shutdown");
            stop();
            return "";
        });

    }

    public static void notifyAll(String job) {
        for (String worker : workerStatus.keySet()) {
            MasterHelper.sendGetRequest(worker, job);
        }
    }

    /**
     * The mainline for launching a MapReduce Master. This should handle at least
     * the status and workerstatus routes, and optionally initialize a worker as
     * well.
     * 
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: MasterServer [port number]");
            System.exit(1);
        }
        int myPort = Integer.valueOf(args[0]);
        port(myPort);

        registerHandlers();
        System.out.println("Master node ready to take requests");
    }

}
