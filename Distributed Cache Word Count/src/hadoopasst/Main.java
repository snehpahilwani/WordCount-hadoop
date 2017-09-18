package hadoopasst;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	
	//run method is implemented as part of Tool class which is extended 
	//for better configuration management. 
	//getConf() is a method to fetch the configuration which can be used easily

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJobName("Word Count - Distributed Cache");

	    // Setting Mapper and Reducer classes
		job.setMapperClass(DistWordMapper.class);
		job.setReducerClass(DistSumReducer.class);
	    // Setting output key value pair classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    // Command Line arguments for input and output and list of words 
		// i.e. the cachefile 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		String cacheFile = args[2];
		job.addCacheFile(new Path(cacheFile).toUri());

		job.setJarByClass(Main.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);

	}
}
