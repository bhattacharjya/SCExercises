package exerciseSC;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;


public class WCMR {
	
	private static String buildRandomString(int len) {
		String characterSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		char[] retCharArray = new char[len];
		for (int i = 0; i < len ; i++){
			int rand = (int)((characterSet.length() - 1) * Math.random());
			retCharArray[i] = characterSet.charAt(rand);
		}
		return new String(retCharArray);
	}
	
	private static boolean wordCountJob(String input, String output, int num_reducers) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize","67108864");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WCMR.class);
		job.setJobName("JumpShot MapReduce Specialised Word Count job");
		
		job.setInputFormatClass(CombineTextInputFormat.class); // Merge the many small email
		                                                       // files into bigger input splits
		FileInputFormat.addInputPath(job, new Path(input));
		
		FileSystem fs = FileSystem.get(conf); // cleanup output before setting it.
		Path tempPath = new Path(output);
		fs.delete(tempPath, true);
		FileOutputFormat.setOutputPath(job, tempPath);	
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setNumReduceTasks(num_reducers);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true);
		
	}
	
    private static boolean sortFinalResultsJob(String input, String output, int num_reducers) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WCMR.class);
		job.setJobName("JumpShot MapReduce sort final output job");
		
		Path inputPath = new Path(input);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, new Path(output));	
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setNumReduceTasks(num_reducers);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		boolean retval = job.waitForCompletion(true);
		FileSystem fs = FileSystem.get(conf); // Cleanup
		fs.delete(inputPath, true);
		
		return retval;
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		if (args.length != 3) {
			System.out.println("Usage : hadoop WCMR <input> <output> <num_reducers>");
			System.exit(1);
		}
		
		int numReducers =  Integer.valueOf(args[2]);
		String temp = "temp" + buildRandomString(6);
		System.out.println("TEMPORARY FILE = " + temp);
		if (wordCountJob(args[0], temp, numReducers)) {
			System.exit(sortFinalResultsJob(temp, args[1], 1) ? 0 : 1);
		}
		
		System.exit(1);

	}
}


