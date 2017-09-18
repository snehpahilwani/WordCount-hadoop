package hadoopasst;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairWordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		StringTokenizer wordList = new StringTokenizer(value.toString());
		String firstWord = "";
		String secondWord = "";
		// Checking if list has tokens and collecting first word of the pair
		if (wordList.hasMoreTokens()) {
			firstWord = wordList.nextToken();
		}
		// Subsequent word collection in pairs taken in the variables
		// firstWord and secondWord which will be considered
		// collectively as keys in this case
		while (wordList.hasMoreTokens()) {
			secondWord = wordList.nextToken();
			word.set(firstWord + " " + secondWord);
			context.write(word, one);
			firstWord = secondWord;
		}
	}
}