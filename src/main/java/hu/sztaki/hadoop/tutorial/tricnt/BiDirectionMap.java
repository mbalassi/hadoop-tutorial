package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//forwards and all outedges and inedges from higher smaller ids
public class BiDirectionMap extends Mapper<LongWritable, Text, LongWritable, Text>
{
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException
      {
        //TODO
      }
}
