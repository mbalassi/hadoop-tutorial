package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMap extends
		Mapper<LongWritable, Text, NullWritable, LongWritable> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(),
				new LongWritable(Long.parseLong(value.toString())));
	}
}
