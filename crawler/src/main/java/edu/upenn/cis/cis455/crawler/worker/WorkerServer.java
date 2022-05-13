package edu.upenn.cis.cis455.crawler.worker;

import static spark.Spark.*;

import java.net.MalformedURLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.Crawler;

/**
 * Simple listener for crawler to work
 */
public class WorkerServer {
    static Logger log = LogManager.getLogger(WorkerServer.class);
    static String master;
    public WorkerReporter backgroundChecker;
    public static String port;
    public Crawler crawler;

    public static void setMaster(String addr) {
        if (addr != null) {
            master = addr;
        }
    }

    public static String getPort() {
        return port;
    }

    public static String getMaster() {
        return master;
    }

    public int getIndexed() {
        return crawler.indexedCount();
    }

    public void registerHandlers() {
        get("/getWork", (request, response) -> {
            String url = request.queryParams("url");
            Crawler.addWork(url);
            return "";
        });

        get("/stop", (request, response) -> {
            crawler.stop();
            log.info("Worker node will stop asking for more work to do");
            return "";
        });

        get("/shutdown", (request, response) -> {
            backgroundChecker.stop();
            crawler.shutdown();
            log.info("Worker node " + port + " has crawled " + crawler.indexedCount() + " in total in this run");
            stop();
            return "";
        });
    };

    public WorkerServer(int myPort, String master) throws MalformedURLException {
        log.info("Creating server listener at socket " + myPort);
        WorkerServer.port = "" + myPort;
        port(myPort);
        registerHandlers();
        setMaster(master);

        backgroundChecker = new WorkerReporter(this);
        crawler = new Crawler(this, 2);
    }

    private void run() {
        new Thread(this.backgroundChecker).start();
        crawler.start();
    }

    /**
     * Simple launch for worker server. Note that you may want to change / replace
     * most of this.
     * 
     * @param args
     * @throws MalformedURLException
     */
    public static void main(String args[]) throws MalformedURLException {
        if (args.length < 2) {
            System.out.println("Usage: WorkerServer [port number] [master host/IP]:[master port]");
            System.exit(1);
        }
        int myPort = Integer.valueOf(args[0]);
        System.out.println("Worker node startup, on port " + myPort);
        WorkerServer worker = new WorkerServer(myPort, args[1]);
        System.out.println("Worker node" + myPort + " ready");
        worker.run();
    }

}
