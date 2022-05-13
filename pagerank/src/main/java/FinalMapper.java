import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<Text, Text, Text, DoubleWritable>{

	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] keyInfo = key.toString().split(",,,");
		if(keyInfo.length != 2) {
			System.err.println("Wrong form of input, should be: link###score");
			return;
		}
        String link = keyInfo[0].strip();
        DoubleWritable score = new DoubleWritable(Double.parseDouble(keyInfo[1]));
        context.write(new Text(link), score);
	}
	
}
