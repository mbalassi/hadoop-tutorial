package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//emits one candidate for each directed triangle
//and passes the structure
public class TrianglesMap extends Mapper<LongWritable, Text, LongWritable, Text>
{
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException
      {
        //TODO
      }
}