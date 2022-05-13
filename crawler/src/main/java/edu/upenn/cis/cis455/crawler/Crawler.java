package edu.upenn.cis.cis455.crawler;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import edu.upenn.cis.cis455.crawler.worker.WorkerServer;
import edu.upenn.cis.stormlite.CrawlerStorm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Crawler {
    final static Logger logger = LogManager.getLogger(Crawler.class);

    public static AtomicInteger indexed = new AtomicInteger();
    public static int maxCnt, maxSize;
    public static BlockingQueue<String> queue = new LinkedBlockingDeque<>();
    public CrawlerStorm crawler = new CrawlerStorm();
    public WorkerServer node;

    public Crawler(WorkerServer node, int size) {
        this.node = node;
        Crawler.maxSize = size;
        try {
            crawler.init();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static BlockingQueue<String> getQueue() {
        return queue;
    }

    public static void getWork() {
        String addr = "http://" + WorkerServer.getMaster() + "/sendWork?port=" + WorkerServer.getPort();
        try {
            URL url = new URL(addr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            int responseCode = conn.getResponseCode();
            System.out.println("Asking for work got response code: " + responseCode);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * We've indexed another document
     */
    public synchronized static void incCount() {
        indexed.getAndIncrement();
    }

    public int indexedCount() {
        return indexed.get();
    }

    public static void addWork(String url) {
        logger.info("url routed from master " + url);
        queue.add(url);
    }

    public void start() {
        getWork();
        this.crawler.run();
    }

    public void shutdown() {
        this.crawler.shutdown();
    }

    public void stop() {
        this.crawler.stop();
    }

}
