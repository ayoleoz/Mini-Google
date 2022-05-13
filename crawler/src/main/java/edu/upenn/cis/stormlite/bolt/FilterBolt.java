package edu.upenn.cis.stormlite.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.Info;
import edu.upenn.cis.cis455.crawler.Rule;
import edu.upenn.cis.cis455.crawler.utils.Parser;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FilterBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(FilterBolt.class);
    public static final int HASH_SIZE = 2048;
    Fields schema = new Fields("URL", "Root", "ContentType");
    CountDownLatch doneSignal;
    boolean toCountDown = false;
    /**
     * map from URL to crawler rule defined by robots.txt
     */
    public Set<String> visitedURLs = new HashSet<>();
    public Map<String, Rule> robotsMap = new ConcurrentHashMap<>();
    public DynamoDB db = new DynamoDB();
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;

    /* ----------------------- Bolt methods ------------------------- */

    public FilterBolt() {
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
        log.info("FilterBolt " + getExecutorId() + " received: " + url);
        String[] canFetchOrNot = canFetch(url);
        log.info("FilterBolt " + getExecutorId() + " finished processing: " + url);
        if (canFetchOrNot[0].equals("1")) {
            if (canFetchOrNot[1] != null) {
                collector.emit(new Values<Object>(url, canFetchOrNot[1], canFetchOrNot[2]));
            }
        }
        if (this.toCountDown) {
            doneSignal.countDown();
        }
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void cleanup() {
        log.info("Filter bolt " + getExecutorId() + " cleans up");
        robotsMap.clear();
        visitedURLs.clear();
    }

    /* ----------------------- Filter methods ------------------------- */

    /*
     * String[0] - if url can be fetched ; String[1] - root of the url for sharding
     * ; String[2] - the content type of the page ;
     */
    public String[] canFetch(String url) {
        if (this.visitedURLs.contains(url)) {
            log.info("This url has been visited before -- skip");
        }
        URLInfo info = new URLInfo(url);
        if (!info.isSecure()) {
            return new String[] { "0", "", "" };
        }
        String root = info.uri();
        if (root.contains("wiki") && !root.contains("en.wikipedia.org")) {
            return new String[] { "0", "", "" };
        }
        Rule visit = null;
        // 1. set up robot exclusion rule for the root domain if needed
        if (!this.robotsMap.containsKey(root)) {
            log.info("check robots rule for " + root);
            visit = Parser.consultRobot(root);
            if (!visit.ruleAdded) {
                return new String[] { "0", "", "" };
            }
            saveCrawlerRule(root, visit);
        } else {
            log.info("found robots rule in memory");
            visit = this.robotsMap.get(root);
        }
        if (visit == null) {
            return new String[] { "0", "", "" };
        }
        log.info("url crawler rule allowed: " + visit.allowed.toString());
        log.info("url crawler rule not allowed: " + visit.notAllowed.toString());
        log.info("current file path: " + info.getFilePath());
        // 2. determine if URL can be crawled by comparing against rule
        if (!visit.canCrawl(info.getFilePath())) {
            log.info("URL " + url + " is not allowed to be crawled -- skip!");
            return new String[] { "0", "", "" };
        }
        // 3. send a HEAD reuqest to see if file type and size matches requirements
        Info headInfo = Parser.fetchInfoByHead(url, info);
        log.info(url + " head info returns");
        if (!Parser.canFetch(headInfo, Crawler.maxSize)) {
            log.info(url + " too big or content type doesn't match -- skip!");
            return new String[] { "0", "", "" };
        }
        // 3. checks if the URL has been crawled before.
        log.info(url + " content type and size match the requirements");
        String lastchecked = db.lastChecked(url);
        if (lastchecked != null) {
            log.info(url + " has been visited and downloaded -- skip");
            return new String[] { "0", "", "" };
        }
        // if moodified, check if should delay crawl, if so add the URL back to the
        // queue
        if (visit.shouldSkip()) {
            log.info("Need to delay crawling, add back to queue");
            Crawler.addWork(url);
            return new String[] { "0", "", "" };
        }
        saveVisitedURL(url);
        return new String[] { "1", info.uri(), headInfo.contentType };
    }

    public void saveVisitedURL(String url) {
        if (this.visitedURLs.size() > HASH_SIZE) {
            log.info("Too many visited urls stored in-memory -- clear");
            this.visitedURLs.clear();
        }
        this.visitedURLs.add(url);
    }

    public void saveCrawlerRule(String uri, Rule rule) {
        if (this.robotsMap.size() >= HASH_SIZE) {
            log.info("Too many crawler rules stored in-memory -- clear");
            this.robotsMap.clear();
        }
        this.robotsMap.put(uri, rule);
    }
}
