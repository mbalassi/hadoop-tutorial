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
		Reducer<LongWritable, Text, LongWritable, NullWritable> {

	public void reduce(LongWritable id, Iterable<Text> destsOrTri,
			Context context) throws IOException, InterruptedException {
		String destStr = "";
		List<String> triCandidates = new ArrayList<String>();
		long triCnt = 0;

		// parsing the input
		String str;
		for (Text text : destsOrTri) {
			str = text.toString();
			// caching triangle candidates
			if (str.startsWith("t")) {
				String[] tPlusCandidate = str.split(" ");
				triCandidates.add(tPlusCandidate[1]);
				// filtering the structure
			} else {
				destStr = str;
			}
		}

		// counting triangles
		String[] dests = destStr.split(" ");
		for (String dest : dests) {
			for (String candidate : triCandidates) {
				if (dest.equals(candidate)) {
					++triCnt;
				}
			}
		}
		// result through a counter
		context.getCounter(Counters.TRI_CNT).increment(triCnt);
		context.write(new LongWritable(triCnt), NullWritable.get());
	}
}
