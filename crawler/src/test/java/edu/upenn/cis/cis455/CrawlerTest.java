package edu.upenn.cis.cis455;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.upenn.cis.cis455.AWS.ContentSeen;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.S3;
import edu.upenn.cis.cis455.AWS.Utils;
import edu.upenn.cis.cis455.AWS.VisitedPage;
import edu.upenn.cis.cis455.AWS.WebLink;
import edu.upenn.cis.cis455.crawler.Rule;
import edu.upenn.cis.cis455.crawler.utils.Parser;
import edu.upenn.cis.cis455.crawler.utils.Serialize;
import edu.upenn.cis.cis455.crawler.utils.URLInfo;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Unit test for simple App.
 */
public class CrawlerTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void splitSetForSQSBatchMessageProcessing() {
        Set<String> h = new HashSet<>();
        for (int i = 0; i < 26; i++) {
            h.add("" + i);
        }
        List<Set<String>> result = Utils.split(h, 10);
        int i = 0;
        for (Set<String> r : result) {
            System.out.println("set " + i++);
            for (String num : r) {
                System.out.println(num);
            }
        }
        assertEquals(3, result.size());
        assertTrue(result.get(0).size() <= 10);
    }

    @Test
    public void describetable() {
        DynamoDbClient ddb = DynamoDB.getDynamoDBClient();
        DescribeTableRequest request = DescribeTableRequest.builder().tableName(DynamoDB.CONTENT_SEEN).build();
        try {
            TableDescription tableInfo = ddb.describeTable(request).table();
            if (tableInfo != null) {
                System.out.format("Table name  : %s\n", tableInfo.tableName());
                System.out.format("Table ARN   : %s\n", tableInfo.tableArn());
                System.out.format("Status      : %s\n", tableInfo.tableStatus());
                System.out.format("Item count  : %d\n", tableInfo.itemCount().longValue());
                System.out.format("Size (bytes): %d\n", tableInfo.tableSizeBytes().longValue());
                List<AttributeDefinition> attributes = tableInfo.attributeDefinitions();
                System.out.println("Attributes");
                for (AttributeDefinition a : attributes) {
                    System.out.format("  %s (%s)\n", a.attributeName(), a.attributeType());
                }
            }
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("\nDone!");
    }

    @Test
    public void testBatchWriteToWebLink() {
        DynamoDbTable<WebLink> mappedTable = DynamoDB.getTable(DynamoDB.WEB_LINKS, DynamoDB.WEB_LINKS_SCHEMA);
        String from = "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryingJavaDocumentAPI.html";
        String to1 = "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SampleData.html";
        String to2 = "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBMapper.html";
        WebLink record1 = new WebLink();
        record1.setFrom(from);
        record1.setTo(to1);
        WebLink record2 = new WebLink();
        record2.setFrom(from);
        record2.setTo(to2);
        DynamoDB db = new DynamoDB();
        List<WebLink> list = Arrays.asList(record1, record2);
        db.batchWrite(WebLink.class, list, mappedTable);
    }

    @Test
    public void testQueryWebLink() {
        DynamoDB db = new DynamoDB();
        Set<String> pointed = db.getWebLink("https://en.wikipedia.org/wiki/Wikipedia:Contents");
        for (String s : pointed) {
            System.out.println("from https://en.wikipedia.org/wiki/Wikipedia:Contents to: " + s);
        }
    }

    @Test
    public void testBatchWriteContentSeen() {
        DynamoDbTable<ContentSeen> mappedTable = DynamoDB.getTable(DynamoDB.CONTENT_SEEN, DynamoDB.CONTENT_SEEN_SCHEMA);
        String hash = "9fad31d15feedf75bb34fd9a081cbdd8";
        String url = "test url";
        ContentSeen content = new ContentSeen();
        content.setHash(hash);
        content.setUrl(url);
        List<ContentSeen> list = Arrays.asList(content);
        DynamoDB db = new DynamoDB();
        db.batchWrite(ContentSeen.class, list, mappedTable);
        // Try getting back
        String exist = hash;
        String nonexist = "6877edaf11ea5688638dc2d04f1ea9b8";
        assertTrue(db.contentSeen(exist));
        assertFalse(db.contentSeen(nonexist));
    }

    @Test
    public void testUploadToS3() {
        S3 s3 = new S3();
        s3.UploadObject("d7060672cd2c2e5c39dab29755df358f6c3f39e2", "test.txt",
                "src/main/resources/executorID1/test.txt");
    }

    @Test
    public void testGetFromS3() {
        S3 s3 = new S3();
        try {
            // args: bucket name, name of file to download, path to create a local file to
            // write to
            s3.getObjectAndWriteToLocal(S3.BUCKET, "test.txt", "src/main/resources/executorID1/downloaded.txt");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ;
    }

    @Test
    public void testGetHTMLFromDynamoDBandS3() {
        S3 s3 = new S3();
        DynamoDB db = new DynamoDB();
        VisitedPage crawledPage1 = db.getCrawledPageInfo("https://crawltest.cis.upenn.edu/nytimes/");
        if (crawledPage1 != null) {
            System.out.println("crawled time: " + crawledPage1.getCrawledTime());
            System.out.println("Page title: " + crawledPage1.getTitle());
            String key = crawledPage1.getS3Key();
            System.out.println("S3 key: " + key);
            long start = crawledPage1.getByteRangeStart();
            long end = crawledPage1.getByteRangeEnd();
            System.out.println("Range: " + start + "-" + end);
            if (key != null) {
                try {
                    String content = s3.getObjectBytes(S3.BUCKET, key, start, end);
                    System.out.println(content);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void testExtractLinks() {
        String url = "https://en.wikipedia.org/wiki/European_nightjar";
        String doc = Parser.fetchContent(url).html();
        List<Set<String>> res = Parser.extractLinks(doc, url);
        Set<String> wiki = res.get(0);
        Set<String> nonwiki = res.get(1);
        for (String w : wiki) {
            System.out.println("wiki page: " + w);
        }
        for (String w : nonwiki) {
            System.out.println("non-wiki page: " + w);
        }
    }

    // @Test
    // public void testSplit() {
    // Set<String> s = new HashSet<>();
    // for (int i = 0; i < 200; i++) {
    // String str = i
    // +
    // "https://www.google.com/search?q=dynamoDB+store+set&oq=dynamoDB+store+set&aqs=chrome..69i57.2591j0j7&sourceid=chrome&ie=UTF-8";
    // s.add(str);
    // }
    // String json = Serialize.convertToJson(s);
    // System.out.println(json);
    // Set<String> set = Serialize.convertBackToSet(json);
    // System.out.println(set.size());
    // }

}
