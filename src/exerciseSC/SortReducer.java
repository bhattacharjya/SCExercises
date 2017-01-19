package exerciseSC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
	
	static final int FLOOR = 9;
	@Override
	public void reduce(IntWritable count, Iterable<Text> words, Context context)
			throws IOException, InterruptedException {
		
		for (Text word : words) {
			context.write(word, count);
		}
	}
	
}
