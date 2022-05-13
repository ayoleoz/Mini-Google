package edu.upenn.cis.cis455.crawler.worker;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkerReporter implements Runnable {
    static final Logger logger = LogManager.getLogger(WorkerReporter.class);
    WorkerServer worker;
    public boolean isInterrupted = false;

    public WorkerReporter(WorkerServer worker) {
        this.worker = worker;
    }

    public void stop() {
        this.isInterrupted = true;
    }

    public void sendStatus() {
        URL url;
        try {
            String master = WorkerServer.getMaster();
            if (master == null) {
                logger.info("Master has not been set yet.");
                return;
            }
            String addr = "http://" + WorkerServer.getMaster() + "/workerstatus?port=" + WorkerServer.port + "&indexed="
                    + worker.getIndexed();
            logger.info("Sending heart beat msg to master: " + addr);
            url = new URL(addr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            int responseCode = conn.getResponseCode();
            logger.info("response code: " + responseCode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (!isInterrupted) {
            sendStatus();
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.info("background listener exists");
    }
}
