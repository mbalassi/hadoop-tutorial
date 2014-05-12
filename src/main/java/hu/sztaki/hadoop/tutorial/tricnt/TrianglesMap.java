package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//emits one candidate for each directed triangle
//and passes the structure
public class TrianglesMap extends
		Mapper<LongWritable, Text, LongWritable, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String vert;
		List<String> dests = new ArrayList<String>();
		List<String> srcs = new ArrayList<String>();

		// parsing the input
		String line = value.toString();
		String buff = "";
		int idx = 0;
		while (idx < line.length() && line.charAt(idx) != ' ') {
			buff += line.charAt(idx++);
		}
		vert = buff;
		buff = "";
		++idx;
		while (idx < line.length() && line.charAt(idx) != '|') {
			buff = "";
			while (idx < line.length() && line.charAt(idx) != ' ') {
				buff += line.charAt(idx++);
			}
			dests.add(buff);
			buff = "";
			++idx;
		}
		++idx;
		while (idx < line.length()) {
			buff = "";
			while (idx < line.length() && line.charAt(idx) != ' ') {
				buff += line.charAt(idx++);
			}
			srcs.add(buff);
			buff = "";
			++idx;
		}

		// emitting triangle candidates and structure
		// covering both 1->2->3 & 3->2->1 type triangles
		String destStr = "";
		for (String dest : dests) {
			destStr += dest + " ";
			if (vert.compareTo(dest) > 0) {
				for (String src : srcs) {
					context.write(new LongWritable(Long.parseLong(dest)),
							new Text("t " + src));
				}
			}
		}

		if (!destStr.equals(""))
			destStr = destStr.substring(0, destStr.length() - 1);
		context.write(new LongWritable(Long.parseLong(vert)), new Text(destStr));
	}
}