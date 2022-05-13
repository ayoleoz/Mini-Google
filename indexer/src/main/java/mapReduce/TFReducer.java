package mapReduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		double total=0;
//		System.out.println(key.toString());
//		System.out.println(values.toString());
		
		for (IntWritable value : values) {
			total += (double) value.get();
//			System.out.println(value.get());
		}
//		System.out.println(key);
//		System.out.println(tf);
		context.write(key, new DoubleWritable(1 + Math.log10(total)));
	}
}
