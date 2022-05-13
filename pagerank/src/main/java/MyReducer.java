import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text>{
	private final DoubleWritable newRank = new DoubleWritable();
	
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    	newRank.set(0.0);
        Text links = new Text("");
        
        for (Text each : values) {
            String valueString = each.toString();
            
            // if not the last write, value should be  weights
            try { 
            	Double weight = Double.parseDouble(valueString);
                newRank.set(weight + newRank.get()) ;
            // if it is the last write, value should be list of links
            } catch (NumberFormatException e) {
                if (valueString.contains("http://")) {
                	links = each; // all outgoing links
                }
            }
        }
        
        // add decay factor to deal with sinks 
        Double decayFactor = 0.85;
        Double beta = 1 - decayFactor;
        newRank.set(newRank.get() * decayFactor + beta);
        
        // if not the last write from mapper
        if (!key.toString().startsWith("http://")) {
        	return;
        } else {
        	if (links.getLength() == 0) { // avoid sink, current link it self is the outgoing links
        		context.write(new Text(key + ",,," + newRank), key);
        	} else {
        		context.write(new Text(key + ",,," + newRank), links);
        	}
        }

    }
}
