package edu.upenn.cis.stormlite.bolt;

import edu.upenn.cis.cis455.crawler.utils.Parser;
import edu.upenn.cis.cis455.crawler.utils.Serialize;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.SQS;
import edu.upenn.cis.cis455.AWS.WebLink;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LinkExtractBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(LinkExtractBolt.class);
    public static final int HASH_SIZE = 1024;
    private OutputCollector collector;
    Fields schema = new Fields("WebLink");
    CountDownLatch doneSignal;
    boolean toCountDown = false;
    String executorId = UUID.randomUUID().toString();
    public DynamoDB db = new DynamoDB();
    // visited urls
    Set<String> visitedURLs = new HashSet<>();

    /* ----------------------- Bolt methods ------------------------- */

    public LinkExtractBolt() {
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
        declarer.declare(schema);
    }

    @Override
    public void cleanup() {
        this.visitedURLs.clear();
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }

    /**
     * Process a tuple received from the stream, fetch the content and outputting
     * result
     */
    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("URL");
        String document = input.getStringByField("Document");
        log.info("LinkExtractBolt " + getExecutorId() + " is going to extract url from: " + url);
        List<WebLink> link = extractLink(url, document);
        if (link != null) {
            for (WebLink l : link) {
                collector.emit(new Values<Object>(l));
                log.info("Sent web link of " + l.getTo() + " to the writer bolt");
            }
        }
        if (this.toCountDown) {
            doneSignal.countDown();
        }
    }

    /* ----------------------- Link Extracting methods ------------------------- */

    public List<WebLink> extractLink(String url, String document) {
        if (this.visitedURLs.contains(url)) {
            return null;
        }
        if (this.visitedURLs.size() > HASH_SIZE) {
            this.visitedURLs.clear();
        }
        this.visitedURLs.add(url);
        List<Set<String>> links = Parser.extractLinks(document, url);
        if (links == null || links.isEmpty() || links.size() != 2) {
            return null;
        }
        List<WebLink> res = new ArrayList<>();
        Set<String> wikiLinks = links.get(0);
        Set<String> otherLinks = links.get(1);
        for (String to : wikiLinks) {
            WebLink wl = new WebLink();
            if (!url.isEmpty() && !to.isEmpty()) {
                wl.setFrom(url);
                wl.setTo(to);
                res.add(wl);
            }
        }
        for (String to : otherLinks) {
            WebLink wl = new WebLink();
            if (!url.isEmpty() && !to.isEmpty()) {
                wl.setFrom(url);
                wl.setTo(to);
                res.add(wl);
            }
        }
        if (wikiLinks.size() > 0) {
            SQS.sendBatchMessages(wikiLinks, SQS.QUEUE_ADDR);
        }
        if (otherLinks.size() > 0) {
            SQS.sendBatchMessages(otherLinks, SQS.QUEUE_TWO_ADDR);
        }
        return res;
    }

}
