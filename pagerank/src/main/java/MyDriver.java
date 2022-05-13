import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class MyDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(MyDriver.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class); // parse by tab
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // each iterations have its own folder
        int iteration = Integer.parseInt(args[0]);
        FileInputFormat.addInputPath(job, new Path("./outputFolder" + iteration + "/"));
        FileOutputFormat.setOutputPath(job, new Path("./outputFolder" + (iteration + 1) + "/"));
        return job.waitForCompletion(true) ? 0 : 1;
	}

}
