package edu.upenn.cis.stormlite.spout;

import java.util.Map;

import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CrawlerQueue implements IRichSpout {
    static Logger log = LogManager.getLogger(CrawlerQueue.class);
    /**
     * The collector is the destination for tuples; you "emit" tuples there
     */
    SpoutOutputCollector collector;
    String executorId = UUID.randomUUID().toString();
    CountDownLatch doneSignal;
    boolean toCountDown = false;
    static boolean stop = false;

    public CrawlerQueue() {
    }

    public static void stop() {
        stop = true;
    }

    public void setLatch(CountDownLatch doneSignal) {
        this.doneSignal = doneSignal;
    }

    public void toCountDown() {
        this.toCountDown = true;
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("URL", "Root"));
    }

    @Override
    public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
        this.collector = collector;
        log.debug("spout " + getExecutorId() + " opening");
    }

    @Override
    public void close() {
        if (Crawler.getQueue() != null) {
            Crawler.getQueue().clear();
        }
    }

    @Override
    public void nextTuple() {
        if (Crawler.getQueue() != null && !Crawler.getQueue().isEmpty()) {
            String url = Crawler.getQueue().poll();
            if (!stop && Crawler.getQueue().size() < 10) {
                Crawler.getWork();
            }
            if (url != null) {
                log.info(getExecutorId() + "crawler queue " + getExecutorId() + " gets url: " + url);
                URLInfo info = new URLInfo(url);
                String root = info.uri();
                if (root != null) {
                    this.collector.emit(new Values<Object>(url, root));
                }
            }
        } else {
            if (!stop) {
                log.info(getExecutorId() + "crawler queue currently empty, ask for work");
                Crawler.getWork();
            } else {
                log.info(getExecutorId() + "wait for current work to be done");
            }
        }
        if (this.toCountDown) {
            doneSignal.countDown();
        }
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

}
