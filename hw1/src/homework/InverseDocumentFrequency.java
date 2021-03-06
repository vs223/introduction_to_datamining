package homework;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;
import java.io.IOException;

public class InverseDocumentFrequency extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new InverseDocumentFrequency(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: <input path> <output path> <number of documents>");
			return -1;
		}

		// create a MapReduce job (put your student id below!)
		Job job = Job.getInstance(getConf(), "InverseDocumentFrequency (2012-11262)");

		// input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// mapper
		job.setMapperClass(IDFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// reducer
		job.setReducerClass(IDFReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// passing number of documents
		job.getConfiguration().setInt("totalDocuments", Integer.parseInt(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * Mapper Implementation for IDF Job
	 *
	 * Input:  key   - a line number (type: LongWritable, not used)
	 *         value - each line in the input file (type: Text)
	 * Output: key   - a word (type: Text)
	 *         value - one (type: IntWritable)
	 */
	public static class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text keyOut = new Text();
		private IntWritable valueOut = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split line by a tap charater to get document ID
			String[] line = value.toString().split("\t");
			int documentId = Integer.parseInt(line[0]);
			// split line by a space character
			String[] words = line[1].split(" ");

			Map<String, String> visitTable = new HashMap<String, String>();
			

			for (String word : words) {
				if(!visitTable.containsKey(word)){
					visitTable.put(word,word);
					// write (word, 1) pair for each word in the line
					keyOut.set(word);
					context.write(keyOut, valueOut);
				}				
			}
		}
	}
	/**
     * Reducer Implementation for IDF Job
     *
     * Input:  key    - a word (type: Text)
     *         values - a list consists of partial word counts from each mapper (type: IntWritable)
     * Output: key    - a word (type: Text)
     *         value  - a idf score (type: DoubleWritable)
     */
	public static class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		// you may assume that this variable contains the total number of documents.
		private int totalDocuments;
		private DoubleWritable valueOut = new DoubleWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			totalDocuments = context.getConfiguration().getInt("totalDocuments", 1);
		}		

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : values) {
				sum += count.get();				
			}
			valueOut.set(Math.log((double)totalDocuments/sum));

			// write a (word, count) pair to output file. The word and count are separated by a tab character.
			context.write(key, valueOut);
		}

	}
}

