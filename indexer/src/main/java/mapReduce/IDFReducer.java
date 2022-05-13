package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.InvertedIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

public class IDFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	private HashMap<String, String> outputMap = new HashMap<String, String>();
//	private ArrayList<String> outputWord = new ArrayList<String>();
//	private ArrayList<String> outputUrl = new ArrayList<String>();
//	private ArrayList<String> outputScore = new ArrayList<String>();
	private List<InvertedIndex> indexLst = new ArrayList<InvertedIndex>();
	DynamoDbTable<InvertedIndex> mappedTable = DynamoDB.getTable("invertedIndexOnDemandv4",
			DynamoDB.TEST_INDEXER_SCHEMA);
	DynamoDB db = new DynamoDB();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		
		int idf = 0;
		ArrayList<Double> tfScores = new ArrayList<Double>();
		ArrayList<String> urls = new ArrayList<String>();
		for (Text value : values) {
			String url = value.toString().split("###")[0];
			double tfscore = Double.parseDouble(value.toString().split("###")[1]);
			tfScores.add(tfscore);
			urls.add(url);
			idf += 1;
		}
//		System.out.println("here");
		int numDoc = context.getConfiguration().getInt("numDoc", 0);
//		System.out.println("Here");
//		System.out.println(numDoc);
		int ind = 0;
		String word = key.toString();
		indexLst = new ArrayList<InvertedIndex>();
		for (String url : urls) {
//			String url = value.toString().split("#####")[0];
//			System.out.println(idf);
//			System.out.println(tfScores.get(ind));
//			System.out.println(Math.log10(numDoc*1.0 / idf));
			double tfidf = Math.log10(numDoc * 1.0 / idf) * tfScores.get(ind);
//			System.out.println(key+"#####"+url+" " + tfidf);
//			context.write(new Text(key + "###" + url), new DoubleWritable(tfidf));
			InvertedIndex record = new InvertedIndex();
			if (word.length() < 100 && url.length() < 200) {
				record.setWord(word);
				record.setUrl(url);
				record.setTfidf(Double.toString(tfidf));
				indexLst.add(record);
			} else {
				System.out.println("word: " + word);
				System.out.println("url: " + url);
			}
//			outputWord.add(key.toString());
//			outputUrl.add(url);
//			outputScore.add(Double.toString(tfidf));

			ind += 1;
		}
		if (indexLst.size() > 0) {
			if (indexLst.size() > 100) {
				Collections.sort(indexLst, new Comparator<InvertedIndex>(){
				     public int compare(InvertedIndex o1, InvertedIndex o2){
				         if(Double.parseDouble(o1.tfidf) == Double.parseDouble(o2.tfidf))
				             return 0;
				         return Double.parseDouble(o1.tfidf) < Double.parseDouble(o2.tfidf) ? -1 : 1;
				     }
				});
				indexLst = indexLst.subList(indexLst.size()-100, indexLst.size());
			}
			try {
				db.batchWrite(InvertedIndex.class, indexLst, mappedTable);
			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}

	public void cleanup(Context context) {
//		System.out.println("uploading");
//		System.out.println(indexLst.size());

//		System.out.println("Got the table");
//		return;
//		InvertedIndex records[] = new InvertedIndex[outputWord.size()];
//		for (int i = 0; i < outputWord.size(); i++) {
//			InvertedIndex record = new InvertedIndex();
//			record.setWord(outputWord.get(i));
//			record.setUrl(outputUrl.get(i));
//			record.setTfidf(outputScore.get(i));
//			records[i] = record;
//		}
//        DynamoDB db = new DynamoDB();
////		List<InvertedIndex> list = Arrays.asList(records);
//		System.out.println("Before batch Writing size of " + indexLst.size());
//		
//		db.batchWrite(InvertedIndex.class, indexLst, mappedTable);
//		System.out.println("finished uploading something");
	}
}
