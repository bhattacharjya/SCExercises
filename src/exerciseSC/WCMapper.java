package exerciseSC;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private static final String END_HEADER  = "X-FileName:";
	private static final String START_HEADER  = "Message-ID:";
	private boolean inHeader = true;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String firstWord;
        StringTokenizer wSplitter = new StringTokenizer(value.toString());
        if (wSplitter.hasMoreTokens()) { // Get the first word in line.
        	firstWord = wSplitter.nextToken().trim();
        	if (inHeader) {
        		if (firstWord.equals(END_HEADER)) {
        			inHeader = false;
        			
        		}
        		return;
        	} else {
        		if (firstWord.equals(START_HEADER)) {
        			inHeader = true;
        			return;
        		}
        		
        	}
        } else { // Line with no tokens.
        	return;
        }
        
        // We are not in the header. Deal with the first word.
        context.write(new Text(firstWord), new IntWritable(1));
        while (wSplitter.hasMoreTokens()) {
        	String word = wSplitter.nextToken();
        	context.write(new Text(word),new IntWritable(1)); 
        }
        
	}

}