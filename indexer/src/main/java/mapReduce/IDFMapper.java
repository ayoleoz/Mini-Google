package mapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class IDFMapper extends Mapper<LongWritable, Text, Text, Text> {
//	private int docCount;
//	private Map<String, HashMap<String, String>> wordToTimes;
//	private Map<String, String> seenUrl;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//		System.out.println(value.toString());
		String urlAndWord = value.toString().split("\t")[0];
		String score = value.toString().split("\t")[1];
		String url = urlAndWord.split("###")[0];
		String word = urlAndWord.split("###")[1];
		

		context.write(new Text(word), new Text(url+"###"+score));

	}
}
