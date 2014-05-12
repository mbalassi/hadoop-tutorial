package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BiDirectionReduce extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable id, Iterable<Text> destsOrSrcs,
			Context context) throws IOException, InterruptedException {
		String destStr = "";
		String srcStr = "";

		for (Text dos : destsOrSrcs) {
			//parse the input into a dest and a source String
		}
		// id with its out- and inedges
		context.write(id, new Text(destStr + "|" + srcStr));
	}
}
