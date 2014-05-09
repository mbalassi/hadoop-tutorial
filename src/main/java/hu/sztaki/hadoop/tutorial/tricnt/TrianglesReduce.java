package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hu.sztaki.hadoop.tutorial.tricnt.DirectedTriangleCounter.Counters;

public class TrianglesReduce extends
		Reducer<LongWritable, Text, NullWritable, NullWritable> {

	public void reduce(LongWritable id, Iterable<Text> destsOrTri,
			Context context) throws IOException, InterruptedException {
		//TODO
		// context.getCounter(Counters.TRI_CNT).increment(triCnt);
	}
}
