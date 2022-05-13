import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import edu.upenn.cis.cis455.AWS.DynamoDB;

public class GetInputFile {

	
	// this method will process the database data and output a text file
	// each line of text file: key|||score "tab" list of outgoing links
	public static void processData(String outputPath) throws IOException {
		// TODO Auto-generated method stub
        DynamoDB db = new DynamoDB();
        Map<String, List<String>> linkMap = db.iterateLinkTable("WEB_LINKS");
        
        if (!Files.exists(Paths.get(outputPath))) {
			try {
				Files.createDirectory(Paths.get(outputPath));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        
        int rowNum = 0;
        FileWriter outputFile = null;
        for(String key : linkMap.keySet()) {
            if((rowNum % 20) == 0) { // 20 rows per text file
                if (outputFile != null) {
                	outputFile.close();
                }
                outputFile = new FileWriter(outputPath + (rowNum / 20) + ".txt");
            }
            // add the tab between mapper key and value
            String mapKey = key + ",,,1.0" + "\t";
            System.out.println(mapKey);
            List<String> mapValues = linkMap.get(key);
            outputFile.write(mapKey);
            for (String each: mapValues) {
            	outputFile.write(each + ",");
            }
            outputFile.write("\n");
            rowNum++;
        }
        outputFile.close();
	}

}
