package edu.upenn.cis.cis455.AWS;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DynamoDB {
	final static Logger logger = LogManager.getLogger(DynamoDB.class);
	// table names
	public static final String CONTENT_SEEN = "content_seen"; // stores MD5 hash of documents crawled
	public static final String WEB_LINKS = "web_link"; // stores the Web links for link analysis?
	public static final String VISITED_URL = "visited_url"; // stores the visited url and link to document and preview
	public static final String TEST_INDEXER = "InvertedIndex"; // stores the inverted index and tfidf score
	public static final String TEST_PAGERANK = "testPageRank"; // stores each url and its pagerank score
	public static final String PAGERANK01 = "pagerank01"; // stores each url and its pagerank score
	private static final int MAX_DYNAMODB_BATCH_SIZE = 10;
	// configurations
	public static final DynamoDbClient client = getDynamoDBClient();
	// declare all table schemas for better efficiency
	public static final TableSchema<CrawlerRule> ROBOTS_RULE_SCHEMA = TableSchema.fromBean(CrawlerRule.class);
	public static final TableSchema<WebLink> WEB_LINKS_SCHEMA = TableSchema.fromBean(WebLink.class);
	public static final TableSchema<ContentSeen> CONTENT_SEEN_SCHEMA = TableSchema.fromBean(ContentSeen.class);
	public static final TableSchema<VisitedPage> VISITED_URL_SCHEMA = TableSchema.fromBean(VisitedPage.class);
	public static final TableSchema<InvertedIndex> TEST_INDEXER_SCHEMA = TableSchema.fromBean(InvertedIndex.class);
	public static final TableSchema<PageRank> TEST_PR_SCHEMA = TableSchema.fromBean(PageRank.class);

	/* --------- Methods for getting DynamoDB client or table for work --------- */
	/**
	 * Get a DynamoDB Client
	 */
	public static DynamoDbClient getDynamoDBClient() {
		return DynamoDbClient.builder().credentialsProvider(StaticCredentialsProvider.create(Utils.awsCreds))
				.region(Region.US_EAST_1).build();
	}

	/**
	 * Get a enhanced DynamoDB Client
	 */
	public static DynamoDbEnhancedClient getEnhancedDynamoDBClient() {
		return DynamoDbEnhancedClient.builder().dynamoDbClient(client).build();
	}

	/**
	 * Get the table to process with, prividing the name of the table and the table
	 * schema
	 *
	 * @param <T>
	 * @param tableName
	 * @param schema
	 * @return
	 */
	public static <T> DynamoDbTable<T> getTable(String tableName, TableSchema<T> schema) {
		DynamoDbEnhancedClient enhancedClient = getEnhancedDynamoDBClient();
		return enhancedClient.table(tableName, schema);
	}

	/* --------- Methods for batch or single writing to DynamoDB tables --------- */
	/**
	 * Batch Write a Set of Items to a DynamoDB Table
	 *
	 * @param <T>
	 * @param itemType
	 * @param items
	 * @param table
	 */
	public <T> void batchWriteSet(Class<T> itemType, Set<T> items, DynamoDbTable<T> table) {
		if (items.isEmpty()) {
			return;
		}
		logger.info("Should batch write " + items.size() + " to table " + table.tableName());
		List<Set<T>> chunksOfItems = Utils.split(items, MAX_DYNAMODB_BATCH_SIZE);
		chunksOfItems.forEach(chunkOfItems -> {
			List<T> unprocessedItems = batchWriteSetImpl(itemType, chunkOfItems, table);
			while (!unprocessedItems.isEmpty()) {
				// some failed (provisioning problems, etc.), so write those again
				try {
					unprocessedItems = batchWriteImpl(itemType, unprocessedItems, table);
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Helper function that batch writes to DynamoDB table
	 */
	private <T> List<T> batchWriteSetImpl(Class<T> itemType, Set<T> chunkOfItems, DynamoDbTable<T> table) {
		if (chunkOfItems.isEmpty()) {
			return new ArrayList<>();
		}
		DynamoDbEnhancedClient client = getEnhancedDynamoDBClient();
		WriteBatch.Builder<T> subBatchBuilder = WriteBatch.builder(itemType).mappedTableResource(table);
		chunkOfItems.forEach(subBatchBuilder::addPutItem);

		BatchWriteItemEnhancedRequest.Builder overallBatchBuilder = BatchWriteItemEnhancedRequest.builder();
		overallBatchBuilder.addWriteBatch(subBatchBuilder.build());

		return client.batchWriteItem(overallBatchBuilder.build()).unprocessedPutItemsForTable(table);
	}

	/**
	 * Batch write a list of items to a dynamoDB table
	 *
	 * @param <T>
	 * @param itemType
	 * @param items
	 * @param table
	 */
	public <T> void batchWrite(Class<T> itemType, List<T> items, DynamoDbTable<T> table) {
		if (items.isEmpty()) {
			return;
		}
		logger.info("Should batch write " + items.size() + " to table " + table.tableName());
		List<ArrayList<T>> chunksOfItems = Utils.split(items, MAX_DYNAMODB_BATCH_SIZE);
		chunksOfItems.forEach(chunkOfItems -> {
			List<T> unprocessedItems = batchWriteImpl(itemType, chunkOfItems, table);
			while (!unprocessedItems.isEmpty()) {
				// some failed (provisioning problems, etc.), so write those again
				unprocessedItems = batchWriteImpl(itemType, unprocessedItems, table);
			}
		});
	}

	/**
	 * Helper function that batch writes to DynamlDB table
	 */
	private <T> List<T> batchWriteImpl(Class<T> itemType, List<T> chunkOfItems, DynamoDbTable<T> table) {
		if (chunkOfItems.isEmpty()) {
			return new ArrayList<>();
		}
		logger.info("writing " + chunkOfItems.size() + " to table " + table.tableName());
		DynamoDbEnhancedClient client = getEnhancedDynamoDBClient();
		WriteBatch.Builder<T> subBatchBuilder = WriteBatch.builder(itemType).mappedTableResource(table);
		chunkOfItems.forEach(subBatchBuilder::addPutItem);

		BatchWriteItemEnhancedRequest.Builder overallBatchBuilder = BatchWriteItemEnhancedRequest.builder();
		overallBatchBuilder.addWriteBatch(subBatchBuilder.build());

		return client.batchWriteItem(overallBatchBuilder.build()).unprocessedPutItemsForTable(table);
	}

	/**
	 * Put an item into a table given table name and a hashmap that maps key names
	 * to key values
	 */
	public void putItemInTable(String tableName, HashMap<String, String> config) {
		DynamoDbClient ddb = getDynamoDBClient();
		HashMap<String, AttributeValue> itemValues = new HashMap<String, AttributeValue>();
		for (String key : config.keySet()) {
			// Add all content to the table
			itemValues.put(key, AttributeValue.builder().s(config.get(key)).build());
		}
		PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(itemValues).build();

		try {
			ddb.putItem(request);
			System.out.println(tableName + " was successfully updated");

		} catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}

	/* ------------- Method for getting results from DynamoDB ---------------- */

	/**
	 * Get the web link structure for URL
	 *
	 * @param partition
	 * @return
	 */
	public Set<String> getWebLink(String url) {
		DynamoDbTable<WebLink> table = getTable(WEB_LINKS, WEB_LINKS_SCHEMA);
		Set<String> to = new HashSet<>();
		try {
			PageIterable<WebLink> links = table
					.query(QueryConditional.keyEqualTo(Key.builder().partitionValue(url).build()));
			if (links == null) {
				return null;
			}
			links.stream().forEach(l -> l.items().forEach(item -> to.add(item.getTo())));
			return to;
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return null;
	}

	/**
	 * Check whether the same content has been crawled before
	 *
	 * @param contentHash
	 * @return
	 */
	public boolean contentSeen(String contentHash) {
		DynamoDbTable<ContentSeen> table = getTable(CONTENT_SEEN, CONTENT_SEEN_SCHEMA);
		try {
			// Create a KEY object
			Key key = Key.builder().partitionValue(contentHash).build();
			// Get the item by using the key
			ContentSeen result = table.getItem(r -> r.key(key));
			return result != null;
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return true;
	}

	/**
	 * Get the last time URL was crawled
	 *
	 * @param url
	 * @return
	 */
	public String lastChecked(String url) {
		DynamoDbTable<VisitedPage> table = getTable(VISITED_URL, VISITED_URL_SCHEMA);
		try {
			// Create a KEY object
			Key key = Key.builder().partitionValue(url).build();
			// Get the item by using the key
			VisitedPage result = table.getItem(r -> r.key(key));
			if (result != null) {
				return result.getCrawledTime();
			}
			return null;
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return null;
	}

	/**
	 * If URL has been crawled, get its corresponding info
	 *
	 * @param url
	 * @return
	 */
	public VisitedPage getCrawledPageInfo(String url) {
		DynamoDbTable<VisitedPage> table = getTable(VISITED_URL, VISITED_URL_SCHEMA);
		try {
			// Create a KEY object
			Key key = Key.builder().partitionValue(url).build();
			// Get the item by using the key
			VisitedPage result = table.getItem(r -> r.key(key));
			return result;
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return null;
	}

	public static void describeDymamoDBTable(String tableName) {
		DynamoDbClient ddb = getDynamoDBClient();
		DescribeTableRequest request = DescribeTableRequest.builder().tableName(tableName).build();
		try {
			TableDescription tableInfo = ddb.describeTable(request).table();
			if (tableInfo != null) {
				System.out.format("Table name  : %s\n", tableInfo.tableName());
				System.out.format("Item count  : %d\n", tableInfo.itemCount().longValue());
			}
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		System.out.println("\nDone!");
	}

	public static ArrayList<String> iterateTable(String tablename, String keyname, int scanStart, int scanEnd) {
		ArrayList<String> ret = new ArrayList<String>();
		DynamoDbClient ddb = getDynamoDBClient();
		// Builder builder = ScanRequest.builder();
		ScanRequest scanRequest = ScanRequest.builder().tableName(tablename).build();
		// DynamoDbTable<VisitedPage> table = getTable(VISITED_URL, VISITED_URL_SCHEMA);
		ScanResponse result = ddb.scan(scanRequest);
		String lastScanned = result.items().get(0).get(keyname).s();
		ArrayList<ScanRequest> allRequests = new ArrayList<ScanRequest>();
		allRequests.add(scanRequest);
		System.out.println("Scanning from " + Integer.toString(scanStart) + " to " + Integer.toString(scanEnd));
		int curScan = 1;
		while (curScan < scanEnd) {
			System.out.println(result.lastEvaluatedKey().toString());
			if (result.lastEvaluatedKey() != null || result.lastEvaluatedKey().size() != 0) {
				// System.out.println(scanRequest.exclusiveStartKey());
				scanRequest = ScanRequest.builder().tableName(tablename).exclusiveStartKey(result.lastEvaluatedKey())
						.build();
				result = ddb.scan(scanRequest);
				allRequests.add(scanRequest);
				curScan += 1;
			} else {
				System.out.println("Scanned everything!!!!!!!");
				System.exit(1);
			}
		}
		System.out.println(allRequests.size());
		// List<ScanRequest> toScanList = allRequests.subList(scanStart, scanEnd);

		// int count = 0;
		// System.out.println("iterating");
		// System.out.println(startFrom);
		// System.out.println(result.items().size());

		for (ScanRequest req : allRequests.subList(scanStart, scanEnd)) {
			result = ddb.scan(req);
			assert (!lastScanned.equals(result.items().get(0).get(keyname).s()));
			for (Map<String, AttributeValue> item : result.items()) {
				// if (count < startFrom) {
				// count += 1;
				// // System.out.println(count);
				// continue;
				// }
				// System.out.println(count);
				ret.add(item.get(keyname).s());
				// count += 1;
				// if (count >= startFrom + 10000) {
				// System.out.println("breaking");
				// System.out.println(startFrom + 10000);
				// System.out.println(count);
				// break;
				// }
			}
		}

		return ret;
	}

	// helper to iterate through the web link table: from, to
	public static void iterateLinkTable(String tableName, String outputPath) throws IOException {
		List<Map> returnL = new ArrayList<>(); // list of maps that store each block of scan
		List<ScanRequest> reqList = new ArrayList<>();
		DynamoDbClient ddb = getDynamoDBClient();
		ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
		ScanResponse result = ddb.scan(scanRequest);
		// result.lastEvaluatedKey();
		reqList.add(scanRequest);
		int count = 0;
		while (!result.lastEvaluatedKey().isEmpty()) {
			scanRequest = ScanRequest.builder().tableName(tableName).exclusiveStartKey(result.lastEvaluatedKey())
					.build();
			result = ddb.scan(scanRequest);
			reqList.add(scanRequest);
			count += 1;
//			 if (count > 2) {
//			 break;
//			 }
			if (count % 20 == 0) {
				System.out.println("Finished scanning " + count + " requests!!!!!!");
			}
		}
		System.out.println("Finished scanning everything!!!!!!");

		int mapidx = 0; // index of map
		// iterate thru scanned request list
		for (ScanRequest req : reqList) {
			result = ddb.scan(req);
			// iterate thru each item in results
			String preFrom = "";
			Map<String, List<String>> res = new HashMap<String, List<String>>(); // key: from, value: to
			
			// fill the map
			for (Map<String, AttributeValue> item : result.items()) {
				if (item.get("from").s().equals(preFrom)) {
					// already have this "from" in the res map
					List<String> preL = res.get(preFrom);
					preL.add(item.get("to").s());
					res.put(item.get("from").s(), preL);
				} else {
					List<String> outlinks = new ArrayList<>();
					outlinks.add(item.get("to").s());
					res.put(item.get("from").s(), outlinks);
				}
				preFrom = item.get("from").s();
			}
			
			// after filling the map, write to text files
			writePageRank(res, outputPath, mapidx);
			if (mapidx%20 == 0) {
				System.out.println("we are writing the " + mapidx + "th map into text files!!!!");
			}
			mapidx++;
		}
	}
	
	// 
	public static void writePageRank(Map<String, List<String>> currMap, String outputPath, int mapidx) throws IOException {
		 int rowNum = 0;
         FileWriter outputFile = null;
         for (String key : currMap.keySet()) {
             if ((rowNum % 1000) == 0) { // 2000 rows per text file
                 if (outputFile != null) {
                     outputFile.close();
                 }
                 outputFile = new FileWriter(outputPath + "batch" + mapidx + "file" + (rowNum / 1000) + ".txt");
             }
             // add the tab between mapper key and value
             String mapKey = key + ",,,1.0" + "\t";
             List<String> mapValues = currMap.get(key);
             outputFile.write(mapKey);
             String value = "";
             for (String each : mapValues) {
                 value += each + ",";

             }
             if (value.length() != 0) {
            	 value = value.substring(0, value.length() - 1); // remove last comma
             }
//             if (rowNum % 100 == 0) {
//            	 System.out.println("value is " + value + "!!!!!!");
//             }
             outputFile.write(value);
             outputFile.write("\n");
             rowNum++;
         }
         outputFile.close();
	}

	// contain minor bugs, remember to change
	// return the set of links that do not have outgoing links
	public static Set<String> noOutLinks(String visited, String weblinks) {
		Set<String> visitedLinks = new HashSet<String>();
		Set<String> webLinks = new HashSet<String>();

		DynamoDbClient ddb = getDynamoDBClient();
		ScanRequest scanRequest1 = ScanRequest.builder().tableName(visited).build();
		ScanResponse result1 = ddb.scan(scanRequest1);
		int count = 0;
		for (Map<String, AttributeValue> item : result1.items()) {
			// System.out.println(item.get("url"));
			visitedLinks.add(item.get("url").s());
			count += 1;
			if (count >= 30) {
				break;
			}
		}

		ScanRequest scanRequest2 = ScanRequest.builder().tableName(weblinks).build();
		ScanResponse result2 = ddb.scan(scanRequest2);
		int count2 = 0;
		for (Map<String, AttributeValue> item : result2.items()) {
			webLinks.add(item.get("from").s());
			count2 += 1;
			if (count2 >= 30) {
				break;
			}
		}

		// remove all weblinks that have outgoing links
		visitedLinks.removeAll(webLinks);
		return visitedLinks;
	}

}
