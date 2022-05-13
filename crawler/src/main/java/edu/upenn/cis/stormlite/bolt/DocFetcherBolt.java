package edu.upenn.cis.stormlite.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import edu.upenn.cis.cis455.AWS.ContentSeen;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.S3;
import edu.upenn.cis.cis455.AWS.VisitedPage;
import edu.upenn.cis.cis455.crawler.Crawler;
import edu.upenn.cis.cis455.crawler.utils.HashFunc;
import edu.upenn.cis.cis455.crawler.utils.Parser;
import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

public class DocFetcherBolt implements IRichBolt {
    static Logger log = LogManager.getLogger(DocFetcherBolt.class);
    public static final int HASH_SIZE = 200;
    public static final int MB = 100;

    public static enum TYPE {
        HTML, XML, PDF
    };

    private OutputCollector collector;
    String executorId = UUID.randomUUID().toString();
    String currentDir;
    String currentFile;
    Fields schema = new Fields("URL", "Document");
    CountDownLatch doneSignal;
    boolean toCountDown = false;
    DynamoDB db = new DynamoDB();
    S3 s3 = new S3();

    /* map from content hash to ContentSeen that will be saved to DynamoDB */
    public Map<String, ContentSeen> contentMap = new ConcurrentHashMap<>();
    /* map from url to VisitedPage that will be saved to DynamoDB */
    public Map<String, VisitedPage> crawledPage = new ConcurrentHashMap<>();
    /* map from pdf link to VisitedPage that will be saved to DynamoDB */
    public Map<String, VisitedPage> pdfPage = new ConcurrentHashMap<>();

    /* ----------------------- Bolt-related methods ------------------------- */

    public DocFetcherBolt() {
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
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public Fields getSchema() {
        return schema;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    @Override
    public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        currentDir = "src/main/resources/" + getExecutorId();
        File dir = new File(currentDir);
        if (!dir.exists()) {
            dir.mkdir();
        }
        currentFile = currentDir + "/" + getExecutorId() + ".txt";
        deleteAndRecreateFile(currentFile);
    }

    @Override
    public void cleanup() {
        log.info("DocFetcher executor " + getExecutorId() + " cleans up by writing everything to DB");
        writeToContentSeen();
        // zipAndWriteToDB();
        writeToDB();
    }

    /**
     * Process a tuple received from the stream, fetch the content and outputting
     * result
     */
    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("URL");
        String contentType = input.getStringByField("ContentType");
        log.info("DocFetcher " + getExecutorId() + " received a " + contentType + " page : " + url);
        String doc = null;
        TYPE type = TYPE.XML;
        if (contentType.contains("text/html")) {
            type = TYPE.HTML;
        } else if (contentType.contains("application/pdf")) {
            type = TYPE.PDF;
        }
        if (type == TYPE.PDF) {
            log.info("shouldn't be able to fetch PDF now");
            // fetchPDF(url);
        } else {
            doc = fetchDocument(url);
            if (type == TYPE.HTML && doc != null) {
                collector.emit(new Values<Object>(url, doc));
                log.info("should extract outgoing links for " + url);
            }
        }
        log.info("DocFetcher " + getExecutorId() + " finishes processign current page " + url);
        if (this.toCountDown) {
            doneSignal.countDown();
        }
    }
    /* -------------------------- Content Fetching ---------------------------- */

    public String fetchDocument(String url) {
        // 1. use the local or saved copy for further process
        Document doc = Parser.fetchContent(url);
        // error might have occurred during crawling
        if (doc == null) {
            return null;
        }
        String content = doc.html();
        if (content == null) {
            return null;
        }
        // - compare the MD5 hash of content
        // - If the MD5 hashed content is seen, do nothing
        String contentHash = HashFunc.MD5(content);
        if (contentSeen(contentHash)) {
            log.info(url + ": same content already exists");
            return null;
        }
        log.info("DocFetcher " + executorId + " is saving: " + url);
        // store and update the corpus
        addDocument(url, doc);
        addContentSeen(contentHash, url);
        Crawler.incCount();
        return content;
    }

    private void fetchPDF(String url) {
        String pdfDir = "./resources/" + getExecutorId() + "/pdf";
        File dir = new File(pdfDir);
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                return;
            }
        }
        String fileName = pdfDir + "/" + getTimeTag() + ".pdf";
        if (Parser.fetchPDF(url, fileName)) {
            // Write to file
            VisitedPage page = new VisitedPage();
            page.setUrl(url);
            page.setCrawledTime(getDate());
            page.setByteRangeStart(0);
            page.setByteRangeEnd(0);
            page.setTitle("");
            this.pdfPage.put(url, page);
        }
    }

    /* ----------------------- save content seen ------------------------- */

    private boolean contentSeen(String content) {
        return this.contentMap.containsKey(content) || db.contentSeen(content);
    }

    public synchronized void addContentSeen(String contentHash, String url) {
        ContentSeen cs = new ContentSeen();
        cs.setHash(contentHash);
        cs.setUrl(url);
        this.contentMap.put(contentHash, cs);
        if (this.contentMap.size() >= HASH_SIZE) {
            writeToContentSeen();
        }
    }

    private void writeToContentSeen() {
        log.info("DocFetcher bolt " + getExecutorId() + " now save the content seen map to database");
        List<ContentSeen> list = new ArrayList<ContentSeen>(this.contentMap.values());
        DynamoDbTable<ContentSeen> mappedTable = DynamoDB.getTable(DynamoDB.CONTENT_SEEN, DynamoDB.CONTENT_SEEN_SCHEMA);
        db.batchWrite(ContentSeen.class, list, mappedTable);
        log.info("DocFetcher bolt " + getExecutorId() + " finished writing to database -- clear table");
        this.contentMap.clear();
    }

    /* ---------------------------- save documents ---------------------------- */

    public boolean addDocument(String url, Document doc) {
        File targetFile = new File(currentFile);
        if (!targetFile.exists()) {
            deleteAndRecreateFile(currentFile);
        }
        if (doc == null) {
            return false;
        }
        String content = doc.html();
        String title = doc.title();
        // Write to file
        VisitedPage page = new VisitedPage();
        page.setUrl(url);
        page.setCrawledTime(getDate());
        long start = targetFile.length();
        page.setByteRangeStart(start);
        page.setByteRangeEnd(start + content.length());
        if (title != null) {
            page.setTitle(title);
        } else {
            page.setTitle("");
        }
        this.crawledPage.put(url, page);
        writeToFile(content);
        return true;
    }

    private void writeToFile(String content) {
        log.info(getExecutorId() + " Now write crawled content to current file");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(currentFile, true));
            writer.append(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        File file = new File(currentFile);
        // Get length of file in bytes
        long fileSizeInBytes = file.length();
        // Convert the bytes to Kilobytes (1 KB = 1024 Bytes)
        long fileSizeInKB = fileSizeInBytes / 1024;
        // Convert the KB to MegaBytes (1 MB = 1024 KBytes)
        long fileSizeInMB = fileSizeInKB / 1024;
        // if file size big enough, save to S3 and recreate the file for next batch
        // writing
        if (fileSizeInMB >= MB) {
            log.info("DocFetcher bolt " + getExecutorId()
                    + " has written enough pages to local file, can now save to S3 and start a new file");
            writeToDB();
            deleteAndRecreateFile(currentFile);
        }
    }

    /*
     * write .txt file to S3 and save every url whose content has been saved in this
     * particular .txt to DynamoDB
     */
    private void writeToDB() {
        File file = new File(currentFile);
        // Get length of file in bytes
        long fileSizeInBytes = file.length();
        if (fileSizeInBytes == 0) {
            log.info("DocFetcher bolt " + getExecutorId() + " file is empty -- skip");
            return;
        }
        String objectName = getExecutorId() + getTimeTag() + ".txt";
        log.info("DocFetcher bolt " + getExecutorId() + " now saves the text file named " + objectName + " to S3");
        s3.UploadObject(S3.BUCKET, objectName, currentFile);
        log.info("set the S3 reference for crawled page");
        for (String url : this.crawledPage.keySet()) {
            VisitedPage page = this.crawledPage.get(url);
            page.setS3Key(objectName);
        }
        log.info("Now save the crawled pages to DB");
        writeToCrawledPage();
    }

    private void writeToCrawledPage() {
        log.info("DocFetcher bolt " + getExecutorId() + " save the VisitedPage map to database");
        List<VisitedPage> list = new ArrayList<VisitedPage>(this.crawledPage.values());
        DynamoDbTable<VisitedPage> mappedTable = DynamoDB.getTable(DynamoDB.VISITED_URL, DynamoDB.VISITED_URL_SCHEMA);
        db.batchWrite(VisitedPage.class, list, mappedTable);
        log.info("DocFetcher bolt " + getExecutorId() + " finished writing to DB -- now clear table");
        this.crawledPage.clear();
    }

    private void writePDFToCrawledPage() {
        log.info("DocFetcher bolt " + getExecutorId() + " save the VisitedPage map to database");
        List<VisitedPage> list = new ArrayList<VisitedPage>(this.pdfPage.values());
        DynamoDbTable<VisitedPage> mappedTable = DynamoDB.getTable(DynamoDB.VISITED_URL, DynamoDB.VISITED_URL_SCHEMA);
        db.batchWrite(VisitedPage.class, list, mappedTable);
        log.info("DocFetcher bolt " + getExecutorId() + " finished writing to DB -- now clear table");
        this.pdfPage.clear();
    }

    /*
     * write .txt file to S3 and save every url whose content has been saved in this
     * particular .txt to DynamoDB
     */
    private void zipAndWriteToDB() {
        log.info("DocFetcher bolt " + getExecutorId() + " now zips all the pdf files it received and upload to S3");
        String pdfDir = currentDir + "/pdf";
        String fileName = getExecutorId() + getTimeTag() + ".zip";
        String outputPath = currentDir + "/" + fileName;

        Parser.zip(pdfDir, fileName, outputPath);
        s3.UploadObject(S3.BUCKET, outputPath, currentFile);
        log.info("set the S3 reference for crawled page");
        for (String url : this.pdfPage.keySet()) {
            VisitedPage page = this.pdfPage.get(url);
            page.setS3Key(fileName);
        }
        log.info("Now save the pdf pages to DB");
        writePDFToCrawledPage();
        log.info("Now clear pdf pages map");
        this.pdfPage.clear();
    }

    /* ------------------------- other helper methods ------------------------ */

    private void deleteAndRecreateFile(String filePath) {
        try {
            Path fileToDeletePath = Paths.get(filePath);
            Files.deleteIfExists(fileToDeletePath);
            File targetFile = new File(filePath);
            if (targetFile.createNewFile()) {
                System.out.println("File created: " + targetFile.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getDate() {
        Date date = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        String strDate = formatter.format(date);
        return strDate;
    }

    private String getTimeTag() {
        Date d = new Date();
        SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat time = new SimpleDateFormat("HH-mm-ss");
        date.setTimeZone(TimeZone.getTimeZone("GMT"));
        time.setTimeZone(TimeZone.getTimeZone("GMT"));
        String strDate = "-" + date.format(d) + "-" + time.format(d);
        return strDate;
    }

}
