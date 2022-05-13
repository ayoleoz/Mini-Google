package mapReduce;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexDriver {
	public static void main(String[] args) throws Exception {
		
//		File f = new File(args[0]);
//        int numDoc = f.list().length;
		int numDoc = 564053;
		
		
		
		
//		Configuration conf = new Configuration();
////		conf.setInt("mapreduce.input.fileinputformat.list-status.num-threads", 64);
//		Job TF = Job.getInstance(conf, "TF");
//		TF.setJarByClass(IndexDriver.class);
//		TF.setMapperClass(TFMapper.class);
//		TF.setReducerClass(TFReducer.class);
//		TF.setMapOutputKeyClass(Text.class);
//		TF.setMapOutputValueClass(IntWritable.class);
//		TF.setOutputKeyClass(Text.class);
//		TF.setOutputValueClass(DoubleWritable.class);
////		TF.setNumReduceTasks(4);
//		FileInputFormat.addInputPath(TF, new Path(args[0]));
//		FileOutputFormat.setOutputPath(TF, new Path(args[1]));
//		TF.waitForCompletion(true);
		
		System.out.println("#########Finished First MapReduce!!!!!!!!!!!!!########");
		
		Configuration conf2 = new Configuration();
		conf2.setInt("numDoc", numDoc);
//		conf2.setInt("mapreduce.input.fileinputformat.list-status.num-threads", 64);
		Job IDF = Job.getInstance(conf2, "IDF");
		IDF.setJarByClass(IndexDriver.class);
		IDF.setMapperClass(IDFMapper.class);
		IDF.setReducerClass(IDFReducer.class);
		IDF.setMapOutputKeyClass(Text.class);
		IDF.setMapOutputValueClass(Text.class);
		IDF.setOutputKeyClass(Text.class);
		IDF.setOutputValueClass(DoubleWritable.class);
		IDF.setNumReduceTasks(128);
		FileInputFormat.addInputPath(IDF, new Path(args[1]));
		FileOutputFormat.setOutputPath(IDF, new Path(args[2]));

		
		System.exit(IDF.waitForCompletion(true) ? 0 : 1);
	}
}
