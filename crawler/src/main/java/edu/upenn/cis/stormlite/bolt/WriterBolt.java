package edu.upenn.cis.stormlite.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.WebLink;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WriterBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(WriterBolt.class);
    public static final int HASH_SIZE = 9;
    Fields schema = new Fields();
    CountDownLatch doneSignal;
    boolean toCountDown = false;
    public List<WebLink> linkMap = new ArrayList<>();
    public DynamoDB db = new DynamoDB();
    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;

    /* ----------------------- Bolt methods ------------------------- */

    public WriterBolt() {
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
        WebLink link = (WebLink) input.getObjectByField("WebLink");
        if (link == null)
            return;
        log.info("Writer bolt " + getExecutorId() + " receives link from " + link.getFrom() + " to: " + link.getTo());
        this.linkMap.add(link);
        if (this.linkMap.size() > HASH_SIZE) {
            writeToWebLinkDB();
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
        log.info("Writer bolt " + getExecutorId() + " cleans up by writing everything to DB");
        writeToWebLinkDB();
    }

    private void writeToWebLinkDB() {
        log.info("Writer bolt " + getExecutorId() + " now save the web link map to database");
        DynamoDbTable<WebLink> mappedTable = DynamoDB.getTable(DynamoDB.WEB_LINKS, DynamoDB.WEB_LINKS_SCHEMA);
        db.batchWrite(WebLink.class, this.linkMap, mappedTable);
        log.info("Writer bolt " + getExecutorId() + " has written to the db - clear the map");
        this.linkMap.clear();
    }

}
