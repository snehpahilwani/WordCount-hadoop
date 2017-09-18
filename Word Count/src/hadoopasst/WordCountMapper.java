package hadoopasst;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	// Serialized Integer Generic Class for Hadoop - IntWritable
	// Notice the Mapper class from which this class is inherited
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	// Main map function taking key value arguments and writing in a context
	// object, thus taking in key-value and returning key-value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Tokenizes the value to extract keys for word count
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			// Setting word object for available tokens and writing in
			// context to pass to reducer
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
