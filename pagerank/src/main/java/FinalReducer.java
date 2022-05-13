import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.upenn.cis.cis455.AWS.DynamoDB;
import edu.upenn.cis.cis455.AWS.PageRank;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;

public class FinalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	private ArrayList<String> outputKey = new ArrayList<String>();
	private ArrayList<String> outputValue = new ArrayList<String>();
	
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
    	
    	// emit link and its final score
        for (DoubleWritable each : values) {
            context.write(key, each);
            outputKey.add(key.toString());
            outputValue.add(Double.toString(each.get()));
            break;
        }
    }
    
    // push data onto database
    public void cleanup(Context context) {
    	DynamoDbTable<PageRank> prTable = DynamoDB.getTable(DynamoDB.TEST_PAGERANK, DynamoDB.TEST_PR_SCHEMA);
    	PageRank records[] = new PageRank [outputKey.size()];
    	for (int i=0; i<outputKey.size(); i++) {
    		PageRank record = new PageRank();
    		record.setUrl(outputKey.get(i));
    		record.setScore(outputValue.get(i));
    		records[i] = record;
    	}
    	DynamoDB db = new DynamoDB();
    	List<PageRank> list = Arrays.asList(records);
    	db.batchWrite(PageRank.class, list, prTable);
    }
}
