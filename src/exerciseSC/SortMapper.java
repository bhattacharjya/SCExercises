package exerciseSC;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		
		int count = Integer.parseInt(value.toString());
		context.write(new IntWritable(count), key); 
		
	}

}