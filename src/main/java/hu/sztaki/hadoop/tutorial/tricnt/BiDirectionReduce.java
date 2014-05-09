package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BiDirectionReduce extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable id, Iterable<Text> destsOrSrcs,
			Context context) throws IOException, InterruptedException {
		// TODO
	}
}
