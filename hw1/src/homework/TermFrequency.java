package homework;

import homework.types.TermDocumentPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.*;

import java.io.IOException;

public class TermFrequency extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TermFrequency(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: <input path> <output path>");
			return -1;
		}

		// create a MapReduce job (put your student id below!)
		Job job = Job.getInstance(getConf(), "TermFrequency (2012-11262)");

		// input
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// mapper
		job.setMapperClass(TFMapper.class);
		job.setMapOutputKeyClass(TermDocumentPair.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// reducer
		job.setNumReduceTasks(0); // we use only mapper for this MapReduce job!
		job.setOutputKeyClass(TermDocumentPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		// output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * Mapper Implementation for TF scoring Job
	 *
	 * Input:  key   - a line number (type: LongWritable, not used)
	 *         value - each line in the input file (type: Text)
	 * Output: key   - a pair of a word and a document id (type: TermDocumentPair)
	 *         value - a TF score (type: DoubleWritable)
	 */
	public static class TFMapper extends Mapper<LongWritable, Text, TermDocumentPair, DoubleWritable> {
		// fill your code here!
		private TermDocumentPair keyOut = new TermDocumentPair();
		private DoubleWritable tfScore = new DoubleWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split line by a tap charater to get document ID
			String[] line = value.toString().split("\t");
			int documentId = Integer.parseInt(line[0]);
			// split line by a space character
			String[] words = line[1].split(" ");

			Map<String, Integer> wordsIndex = new HashMap<String, Integer>();
			ArrayList<Integer> wordsCount = new ArrayList<Integer>();
			int maxCount = 1;

			for(String word : words){
				if(wordsIndex.containsKey(word)){
					int wordIndex = wordsIndex.get(word);
					int wordCount = wordsCount.get(wordIndex)+1;
					wordsCount.set(wordIndex, wordCount);
					if(maxCount < wordCount){
						maxCount = wordCount; 
					}
				}
				else{
					wordsIndex.put(word,wordsCount.size());
					wordsCount.add(1);
				}
			}

			for (String word : wordsIndex.keySet()) {
				// write (word, 1) pair for each word in the line
				int wordIndex = wordsIndex.get(word);
				int wordCount = wordsCount.get(wordIndex);
				keyOut.set(word, documentId);
				tfScore.set(0.5 + 0.5 * wordCount/maxCount);
				context.write(keyOut, tfScore);                
			}
		}
	}
}

