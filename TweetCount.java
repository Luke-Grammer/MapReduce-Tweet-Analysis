/**
 * TweetCount is a MapReduce application that uses a provided twitter dataset to analyze 
 * how many tweets are uploaded during each hour
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * TweetCount contains custom hadoop MapReduce functions
 * @author Luke Grammer
 */
public class TweetCount {
	/**
	 * Custom mapper class for hadoop takes a tweet record, checks the tweet timestamp, and sends the hour and occurrence to the reducer
	 * @author Luke Grammer
	 */
	public static class TimeMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text hour = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Start with a tweet record
			String record = value.toString();
			
			String[] lines = record.split("\n");
			if (lines.length < 3)
				throw new IOException("Invalid Tweet Record:\n" + lines);
			
			int timestamp_line_idx = lines[0].toUpperCase().startsWith("TOTAL NUMBER:") ? 1 : 0;
			String timestamp = lines[timestamp_line_idx].trim().toUpperCase();

			// Timestamps start with T and have three terms separated by ' ' where the last term is three terms separated by ':' 
			if (timestamp.startsWith("T") && timestamp.split("\\s+").length == 3) {
				if (timestamp.split("\\s+")[2].split(":").length == 3) {	
					hour.set(timestamp.split("\\s+")[2].split(":")[0]);
					context.write(hour, new IntWritable(1));					
				}
				else throw new IOException("Tweet timestamp is improperly formatted: " + timestamp);
			}
			else throw new IOException("Tweet timestamp is improperly formatted: " + timestamp);
		}	
	}

	/**
	 * Custom mapper class for hadoop takes a tweet record, checks the tweet timestamp, and sends the hour and occurrence to the reducer only if the tweet text contains the word 'sleep' (case insensitive)
	 * @author Luke Grammer
	 */
	public static class SleepTimeMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text hour = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Start with a tweet record
			String record = value.toString();
			
			String[] lines = record.split("\n");
			if (lines.length < 3)
				throw new IOException("Invalid Tweet Record:\n" + lines);
			
			int timestamp_line_idx = lines[0].trim().toUpperCase().startsWith("TOTAL NUMBER:") ? 1 : 0;
			String timestamp = lines[timestamp_line_idx].trim().toUpperCase();
			
			// Timestamps start with T and have three terms separated by ' ' where the last term is three terms separated by ':' 
			if (timestamp.startsWith("T") && timestamp.split("\\s+").length > 2) {
				if (timestamp.split("\\s+")[2].split(":").length == 3) {	
					hour.set(timestamp.split("\\s+")[2].split(":")[0]);
					String content = "";
					for (int i = 2 + timestamp_line_idx; i < lines.length; i++)
						content += lines[i];

					if (content.startsWith("W")) {
						content = content.toUpperCase();
						if (content.contains("SLEEP")) {
							context.write(hour, new IntWritable(1));												
						}
					}
					else throw new IOException("Tweet content is improperly formatted:\n" + content);
				}
				else throw new IOException("Tweet timestamp is improperly formatted: " + timestamp);
			}
			else throw new IOException("Tweet timestamp is improperly formatted: " + timestamp);
		}
	}
	
	/**
	 * Custom reducer class sums up tweet occurrences for each hour
	 * @author Luke Grammer
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			throw new Exception("Incorrect arguments provided!\n Usage: TweetCount <INPUT_PATH> <OUTPUT_PATH> <PROBLEM_BOOL>");
		}
		Configuration conf = new Configuration();

		// As an alternative to a custom RecordReader class, a custom record delimiter can be set like so:
		conf.set("textinputformat.record.delimiter", "\n\n");
  
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(TweetCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if (args[2].equals("0")) {
			job.setMapperClass(TimeMap.class);
		}
		else if (args[2].equals("1")) {
			job.setMapperClass(SleepTimeMap.class);
		}
		else {
			throw new Exception("Incorrect arguments provided!\n Usage: WordCount <INPUT_PATH> <OUTPUT_PATH> <PROBLEM_BOOL>");
		}

		job.setReducerClass(Reduce.class);
  
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
			  
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
  
		job.waitForCompletion(true);
	}
}
