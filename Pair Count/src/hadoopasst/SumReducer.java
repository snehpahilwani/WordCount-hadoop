package hadoopasst;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable totalPairCount = new IntWritable();

	//Reducer function iterates over the received results from the mapper
	// increases pair count with every pair encountered and collects sum
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int pairCount = 0;
		for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
			pairCount += it.next().get();
		}
		totalPairCount.set(pairCount);
		context.write(key, totalPairCount);
	}
}
