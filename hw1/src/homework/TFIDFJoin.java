package homework;

import homework.types.TermDocumentPair;
import homework.types.TypedRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.DelegatingInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.*;
import java.io.IOException;

public class TFIDFJoin extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TFIDFJoin(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: <tf score path> <idf score path> <output path>");
			return -1;
		}

		// create a MapReduce job (put your student id below!)
		Job job = Job.getInstance(getConf(), "TFIDFJoin (2012-11262)");

		// input & mapper
		job.setInputFormatClass(DelegatingInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TFMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IDFMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TypedRecord.class);

		// reducer
		job.setReducerClass(TFIDFReducer.class);
		job.setOutputKeyClass(TermDocumentPair.class);
		job.setOutputValueClass(DoubleWritable.class);

		// output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * Mapper Implementation for converting TF scores to TF entry Job
	 *
	 * Input:  key   - a line number (type: LongWritable, not used)
	 *         value - each line in the input file (type: Text)
	 * Output: key   - a word (type: Text)
	 *         value - a TF TypedRecord (type: TypedRecord)
	 */
	public static class TFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
		private Text keyOut = new Text();
		private TypedRecord valueOut = new TypedRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split line by a tab character
			String line = value.toString();
			String[] args = line.split("\t");
			keyOut.set(args[0]);
			valueOut.setTFScore(Integer.parseInt(args[1]),Double.parseDouble(args[2]));
			context.write(keyOut,valueOut);
		}
	}
	/**
	 * Mapper Implementation for converting IDF scores to IDF entry Job
	 *
	 * Input:  key   - a line number (type: LongWritable, not used)
	 *         value - each line in the input file (type: Text)
	 * Output: key   - a word (type: Text)
	 *         value - a IDF TypedRecord (type: TypedRecord)
	 */
	public static class IDFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
		private Text keyOut = new Text();
		private TypedRecord valueOut = new TypedRecord();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split line by a tab character
			String line = value.toString();
			String[] args = line.split("\t");
			keyOut.set(args[0]);
			valueOut.setIDFScore(Double.parseDouble(args[1]));
			context.write(keyOut,valueOut);
		}
	}

	{
		// fill your code here!
	}

	/**
	 * Reducer Implementation for TFIDF Job
	 *
	 * Input:  key    - a word (type: Text)
	 *         values -  a list consists of TypedRecords (type: TypedRecord)
	 * Output: key    - a pair of a word and a document id (type: TermDocumentPair)
	 *         value  - a tfidf score (type: DoubleWritable)
	 */
	public static class TFIDFReducer extends Reducer<Text, TypedRecord, TermDocumentPair, DoubleWritable>  {
		private TermDocumentPair keyOut = new TermDocumentPair();
		private DoubleWritable valueOut = new DoubleWritable();

		private class TypedRecordComparator implements Comparator<TypedRecord>{
			@Override
			public int compare(TypedRecord a, TypedRecord b){
				if(a.getScore() - b.getScore() > 0){
					return -1;
				}
				else if(b.getScore() - a.getScore() > 0){
					return 1;
				}
				return 0;
			}
		}

		@Override
		protected void reduce(Text key, Iterable<TypedRecord> values, Context context) throws IOException, InterruptedException {
			
			Comparator<TypedRecord> recordComparator = new TypedRecordComparator();
			PriorityQueue<TypedRecord> maxHeap = new PriorityQueue<TypedRecord>(100,recordComparator);
			double idf_score = 0;
			int n = 10;

			for (TypedRecord record : values) {
				if(record.getType() == TypedRecord.RecordType.TF){
					maxHeap.add(new TypedRecord(record));
				}
				else{
					idf_score = record.getScore();
				}
			}
			
			while(n-- > 0 && maxHeap.peek() != null){
				TypedRecord record = maxHeap.poll();
				keyOut.set(key.toString(), record.getDocumentId());
				valueOut.set(record.getScore() * idf_score);
				context.write(keyOut, valueOut);
			}		
			
		}
	}
}

