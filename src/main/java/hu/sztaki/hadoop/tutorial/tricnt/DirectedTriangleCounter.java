package hu.sztaki.hadoop.tutorial.tricnt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//invoke with args: input output numReducers
public class DirectedTriangleCounter extends Configured implements Tool
{
	
	static enum Counters {
        TRI_CNT
    }

    public static final String TRI_CNT_AGG = "TRI_CNT";
	
  public int run(String[] args) throws Exception
  {
    //cleanup
    Configuration conf = getConf();
    conf.set("mapred.textoutputformat.separator", " ");

    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path("temp/dirtri-temp1"), true);
    fs.delete(new Path("temp/dirtri-temp2"), true);
    fs.delete(new Path(args[1]), true);

    Job job1 = new Job(conf);
    job1.setJobName("dirtri-bidirection");

    job1.setMapOutputKeyClass(LongWritable.class);
    job1.setMapOutputValueClass(Text.class);

    job1.setOutputKeyClass(LongWritable.class);
    job1.setOutputValueClass(Text.class);

    job1.setJarByClass(DirectedTriangleCounter.class);
    job1.setMapperClass(BiDirectionMap.class);
    job1.setReducerClass(BiDirectionReduce.class);
    job1.setNumReduceTasks(Integer.parseInt(args[2]));

    job1.setInputFormatClass(TextInputFormat.class);
    job1.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp/dirtri-temp1"));

    Job job2 = new Job(getConf());
    job2.setJobName("dirtri-triangles");

    job2.setMapOutputKeyClass(LongWritable.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setOutputKeyClass(LongWritable.class);
    job2.setOutputValueClass(LongWritable.class);

    job2.setJarByClass(DirectedTriangleCounter.class);
    job2.setMapperClass(TrianglesMap.class);
    job2.setReducerClass(TrianglesReduce.class);
    job2.setNumReduceTasks(Integer.parseInt(args[2]));

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job2, new Path("temp/dirtri-temp1"));
    FileOutputFormat.setOutputPath(job2, new Path("temp/dirtri-temp2"));

    Job job3 = new Job(getConf());
    job3.setJobName("dirtri-counter");

    job3.setMapOutputKeyClass(NullWritable.class);
    job3.setMapOutputValueClass(LongWritable.class);

    job3.setOutputKeyClass(LongWritable.class);
    job3.setOutputValueClass(LongWritable.class);

    job3.setJarByClass(DirectedTriangleCounter.class);
    job3.setMapperClass(SumMap.class);
    job3.setReducerClass(SumReduce.class);
    job3.setNumReduceTasks(Integer.parseInt(args[2]));

    job3.setInputFormatClass(TextInputFormat.class);
    job3.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job3, new Path("temp/dirtri-temp2"));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));

    int ret = job1.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
    if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
    return ret;
  }

  public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new DirectedTriangleCounter(), args);
    System.exit(res);
  }

}
