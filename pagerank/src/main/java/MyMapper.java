import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Text, Text, Text, Text> {
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// format of key: current page url,pagerank score
		// format of value; list of outgoing links
		System.out.println("original string is " + key.toString());
		String[] keyInfo = key.toString().split(",,,");
		System.out.println("key is " + keyInfo[0]);
		System.out.println("number is " + keyInfo[1]);
		String link = keyInfo[0];
		Double score = Double.parseDouble(keyInfo[1]);
		String[] outgoings = value.toString().split(",");
        for(String each: outgoings) {
            double weight = ( (double)score / outgoings.length);
            String weightString = String.valueOf(weight);
            context.write(new Text(each), new Text(weightString));
        }
        context.write(new Text(link), value); // keep record of the current link and all outgoing links
	}
}
