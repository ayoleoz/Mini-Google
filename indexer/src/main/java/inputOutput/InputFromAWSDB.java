package inputOutput;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Cleaner;
import org.jsoup.safety.Safelist;

import edu.upenn.cis.cis455.AWS.ContentSeen;
import edu.upenn.cis.cis455.AWS.CrawlerRule;
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
import java.time.format.DateTimeFormatter;  
import java.time.LocalDateTime; 

public class InputFromAWSDB {
	static int scanStart;
	static int batchID;
	static int docID=0;
	
	private static String cleanHTML(String html) {
		Document doc = Jsoup.parse(html);
		String content = doc.text();
		String cleaned = content.replaceAll("[^a-zA-Z ]", "").toLowerCase();
		
		return cleaned;
	}
	
	private static void saveCleanedHTML(String url, String file, String dir, String idToUrlFile) {
		if (!Files.exists(Paths.get(dir))) {
			try {
				Files.createDirectory(Paths.get(dir));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		PrintWriter writer;
		try {
			writer = new PrintWriter(dir + "input." + batchID+"." + docID+".txt", "UTF-8");
			writer.println(url);
			writer.println(file);
			writer.close();
			
			FileWriter fw = new FileWriter(dir+"../idToURLFiles/"+ idToUrlFile, true);
		    BufferedWriter bw = new BufferedWriter(fw);
		    bw.write(batchID +"." + docID + " " + url);
		    bw.newLine();
		    bw.close();
		    if (docID%100==0) {
	        	System.out.println("Finished writing " + docID + " pages.");
	        	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
	            LocalDateTime now = LocalDateTime.now();  
	            System.out.println(dtf.format(now)); 
	        }
	        docID+=1;
	        
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
//		
//		String path = dir+url;
//		File outputFile = Paths.get(path).toFile();
//		try {
//			if (!outputFile.exists()) {
//				outputFile.createNewFile();
//	        }
//			Files.write(Paths.get(path), file.getBytes(StandardCharsets.UTF_8));
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

	public static void main(String args[]) {
		String saveDir = args[0];
		batchID = Integer.parseInt(args[1]);
		scanStart = Integer.parseInt(args[2]);
		
		S3 s3 = new S3();
        DynamoDB db = new DynamoDB();
//        System.out.println("1");
//        System.out.println(docID);
        ArrayList<String> urls = db.iterateTable("visited_url", "url", scanStart,scanStart+6);
        System.out.println("Finished reading from Dynamo DB");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");  
        LocalDateTime now = LocalDateTime.now();  
        System.out.println(dtf.format(now)); 
        System.out.println(urls.size());
//        System.out.println("2");
        for (String url :urls) {
//        	System.out.println("3");
        	VisitedPage crawledPage1 = db.getCrawledPageInfo(url);
//        	System.out.println("4");
        	String content = "";
            if (crawledPage1 != null) {
//                System.out.println("crawled time: " + crawledPage1.getCrawledTime());
//                System.out.println("Page title: " + crawledPage1.getTitle());
//            	System.out.println("3");
                String key = crawledPage1.getS3Key();
//                System.out.println("S3 key: " + key);
                long start = crawledPage1.getByteRangeStart();
                long end = crawledPage1.getByteRangeEnd();
//                System.out.println("4");
//                System.out.println("Range: " + start + "-" + end);
                
                if (key != null) {
                    try {
                        content = s3.getObjectBytes(S3.BUCKET, key, start, end);
//                        System.out.println(content);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            if (content!=null&& !content.equals("")) {
//            	System.out.println(content);
//            	System.out.println("\n");
            	//System.out.println(content);
            	String cleaned = cleanHTML(content);
            	saveCleanedHTML(url, cleaned, saveDir, "idToURL."+Integer.toString(batchID)+".txt");
            }
        }
	}
}
