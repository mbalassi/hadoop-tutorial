package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReduce extends
		Reducer<NullWritable, LongWritable, LongWritable, NullWritable> {

	public void reduce(NullWritable nullWritable, Iterable<LongWritable> triCnts,
			Context context) throws IOException, InterruptedException {
		long triCnt = 0;

		for (LongWritable cnt: triCnts) {
			triCnt += cnt.get();
		}
		context.write(new LongWritable(triCnt), NullWritable.get());
	}
}
