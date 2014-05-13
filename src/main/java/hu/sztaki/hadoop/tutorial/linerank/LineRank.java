package hu.sztaki.hadoop.tutorial.linerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LineRank {

	public static final double RESTART_PROB = 0.15;
	public static final double EPS = 0.000000001;
	public static final String EDGE_COUNT = "EDGE_COUNT";

	static enum Counter {
		EDGE_COUNT, MORE_ITERATIONS
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.out
					.println("Arguments: <input file> <output dir> <temp dir> [<reducer count>]");
			return;
		}

		String input = args[0];
		String output = args[1];
		String tempDir = args[2].endsWith("/") ? args[2] : args[2] + "/";
		int reducerCount = args.length < 4 ? 0 : Integer.parseInt(args[3]);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(output), true);
		fs.delete(new Path(tempDir), true);

		int i = 0;
		// Input processing job
		Job job = new Job(conf, "Linerank input processing");
		// Configure mapper
		job.setInputFormatClass(TextInputFormat.class);
		//job.setMapperClass(InputMapper.class);
		FileInputFormat.addInputPath(job, new Path(input));
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		// Configure reducer
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		// job.setReducerClass(InputReducer.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(tempDir + i));
		// Run job
		job.setJarByClass(LineRank.class);
		job.waitForCompletion(true);

		conf.setLong(EDGE_COUNT,
				job.getCounters().findCounter(Counter.EDGE_COUNT).getValue());

		while (true) {
			String tempFileIn = tempDir + i;
			++i;
			String tempFileOut = tempDir + i;

			// Iterated job
			job = new Job(conf, "Linerank calculation, iteration: " + i);
			// Configure mapper
			job.setInputFormatClass(SequenceFileInputFormat.class);
			//job.setMapperClass(IteratedMapper.class);
			FileInputFormat.addInputPath(job, new Path(tempFileIn));
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(BytesWritable.class);
			// Configure combiner
			//job.setCombinerClass(IteratedCombiner.class);
			// Configure reducer
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(BytesWritable.class);
			//job.setReducerClass(IteratedReducer.class);
			FileOutputFormat.setOutputPath(job, new Path(tempFileOut));
			if (reducerCount > 0) {
				job.setNumReduceTasks(reducerCount);
			}
			// Run job
			job.setJarByClass(LineRank.class);
			job.waitForCompletion(true);

			// Delete intermediate results
			fs.delete(new Path(tempFileIn), true);

			if (job.getCounters().findCounter(Counter.MORE_ITERATIONS)
					.getValue() == 0) {
				input = tempFileOut;
				break;
			}
		}

		// Output processing job
		job = new Job(conf, "Linerank output processing");
		// Configure mapper
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setMapperClass(OutputMapper.class);
		FileInputFormat.addInputPath(job, new Path(input));
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		// Configure combiner
		//job.setCombinerClass(IteratedCombiner.class);
		// Configure reducer
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		//job.setReducerClass(OutputReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		// Run job
		job.setJarByClass(LineRank.class);
		job.waitForCompletion(true);

		// Delete intermediate results
		fs.delete(new Path(input), true);
	}

}
