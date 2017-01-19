package exerciseSC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	static final int FLOOR = 9;
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		
		int wordCount = 0;
		
		for (IntWritable count : counts) { // For each word
			wordCount += count.get();
		}
		if (wordCount > FLOOR) {
			context.write(key,new IntWritable(wordCount));
		}
	}
	
}
