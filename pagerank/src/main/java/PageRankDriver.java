import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver {

	public PageRankDriver() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws Exception {
		
		GetInputFile.processData("./outputFolder0/");
		
		// pagerank
		int iterations = 0;
		int fail = 0;
		while (iterations < 15) {
			String[] arguments = new String[] { Integer.toString(iterations) };
			fail = ToolRunner.run(new MyDriver(), arguments);
			// if we failed to run mapreduce
			if (fail == 1) { 
				System.err.println("PageRank failed during " + iterations + "th iteration!");
				System.exit(fail);
			}
			iterations++;
		}
		
		String[] arguments = new String[] { Integer.toString(iterations) };
		fail = ToolRunner.run(new FinalDriver(), arguments);
		// if we failed to run final mapreduce
		if (fail == 1) { 
			System.err.println("Final round failed!");
			System.exit(fail);
		}
		
		System.out.println("PageRank done!");

	}

}
