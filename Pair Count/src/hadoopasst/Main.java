package hadoopasst;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//Initialize configuration
		Job job = Job.getInstance(new Configuration()); 
		job.setJobName("PairWord Count");
	    // Setting Mapper and Reducer classes
		job.setMapperClass(PairWordMapper.class); 
		job.setReducerClass(SumReducer.class); 
	    // Setting output key value pair classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		//Taking input and output from command line
		FileInputFormat.setInputPaths(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		job.setJarByClass(Main.class); 
		System.exit(job.waitForCompletion(true) ? 0 : 1); 

	}

}
