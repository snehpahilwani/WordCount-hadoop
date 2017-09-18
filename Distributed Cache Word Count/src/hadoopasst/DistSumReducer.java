package hadoopasst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable totalDistWordCount = new IntWritable();
	//Same logic used as previous reducer programs 
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int distWordCount = 0;
		for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
			distWordCount += it.next().get();
		}
		totalDistWordCount.set(distWordCount);
		context.write(key, totalDistWordCount);
	}
}
