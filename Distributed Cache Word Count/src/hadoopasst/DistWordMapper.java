package hadoopasst;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class DistWordMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);

	List<String> searchWordList = new ArrayList<String>();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// Tokenizing strings
		StringTokenizer wordList = new StringTokenizer(value.toString());
		String currentToken = null;
		// Checking if word is present in word list and generating
		// key value pair with count
		while (wordList.hasMoreTokens()) {
			currentToken = wordList.nextToken();
			if (searchWordList.contains(currentToken)) {
				word.set(currentToken);
				context.write(word, one);
			}
		}
	}

	// Found it important to override this method as it runs before map job 
	// to read from cached file. It runs readFile multiple times for every mapper task
	// Not required in earlier programs as no external file involvement.
	// This is basically the starting point before map operations run.
	@Override
	public void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Path[] searchListFiles = new Path[2];
		try {
			//fetches absolute path of cached files on nodes
			searchListFiles = context.getLocalCacheFiles(); 
		} catch (IOException e) {
			e.printStackTrace();
		}
		readFile(searchListFiles[0].toString());
		super.setup(context);

	}

	// Method to reading cached file entries and populating the 
	// searchWordList list to be used further.
	public void readFile(String patternFile) {
		try {
			//Generic BufferedReader syntax to read a file
			BufferedReader reader = new BufferedReader(new FileReader(patternFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				StringTokenizer itr = new StringTokenizer(line);
				String token = null;
				while (itr.hasMoreTokens()) {
					token = itr.nextToken();
					searchWordList.add(token);
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}