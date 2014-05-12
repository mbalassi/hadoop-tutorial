package hu.sztaki.hadoop.tutorial.tricnt;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//forwards and all outedges and inedges from higher to smaller ids
public class BiDirectionMap extends Mapper<LongWritable, Text, LongWritable, Text>
{
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException
          {
              String src;

              String line = value.toString();
              String num = "";
              int idx = 0;
              while (idx < line.length() && line.charAt(idx) != ' ') {
                num += line.charAt(idx++);
              }
              src = num;
              num = "";
              ++idx;
              while (idx < line.length()) {
                num = "";
                while (idx < line.length() && line.charAt(idx) != ' ') {
                  num += line.charAt(idx++);
                }
                
                //TODO: Emit the structure of the graph
                
                if (num.compareTo(src) > 0){
              	
                	//TODO: Emit inedges from higher to smaller ids, make sure that you
                	//you will be able to differentiate between the two type of inputs!
                
                }
                num = "";
                ++idx;
              }
            }
}
