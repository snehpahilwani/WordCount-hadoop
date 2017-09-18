package hadoopasst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
// Main class to call mapper and reducer
public static void main(String[] args) throws Exception {
	// Configuration starting for MR 
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word Count");
    job.setJarByClass(Main.class);
    // Setting Mapper and Reducer classes
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    // Setting output key value pair classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    // Command Line arguments for input and output
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // To quit program on successful job completion 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
