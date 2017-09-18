package hadoopasst;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	//Reducer class is the main class. This class inherits that to implement
	//reducer for our algo
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			//Counting sum for the key values retained from the mapper
			sum += val.get();
		}
		result.set(sum);
		//Writing to output key and result
		context.write(key, result);
	}
}
